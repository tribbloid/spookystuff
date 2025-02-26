package com.tribbloids.spookystuff.actions

import ai.acyclic.prover.commons.cap.Capability
import ai.acyclic.prover.commons.cap.Capability.<>
import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Foundation.{HasTrace, TraceSet}
import com.tribbloids.spookystuff.actions.Trace.Repr
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.caching.{DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.row.SpookySchema

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

object Trace {

  private type Repr = List[Action]

  implicit def unbox(v: Trace): Repr = v.self
  implicit def box(v: Repr): Trace = Trace(v)

  type DryRun = List[Trace]

  def of(vs: Action*): Trace = Trace(vs.toList)

  case class Rollout(trace: Trace) extends HasTrace with SpookyContext.Contextual {
    // unlike trace, it is always executed by the agent from scratch
    // thus, execution result can be cached, as replaying it will most likely have the same result (if the trace is deterministic)

    import Rollout.*

    /**
      * deliberately NON-transient to be included in serialized from
      *
      * but can be manually discarded by calling [[uncache]] to make serialization even faster
      *
      * assuming that most [[Observation]]s will be auto-saved into DFS, their serialized form can exclude contents and
      * thus be very small, making such manual discarding an optimisation with diminishing return
      */
    @volatile private var _cached: Seq[Observation] = {
      if (trace.isEmpty) Nil
      else null
    }

    def cachedOpt: Option[Seq[Observation]] = Option(_cached)

    def enableCached: Rollout <> Cached = Capability(this) <> Cached
    def disableCached: Rollout = this.asInstanceOf[Rollout]

    def cache(vs: Seq[Observation]): Rollout <> Cached = {
      this._cached = vs
      this.enableCached
    }

    def uncache: Rollout = {
      this._cached = null
      this.disableCached
    }

    case class _WithCtx(spooky: SpookyContext) extends NOTSerializable {

      def play(): Seq[Observation] = {
        val result = trace.fetch(spooky)
        Rollout.this._cached = result
        result
      }

      lazy val cached: Rollout <> Cached = {

        Trace.this.synchronized {

          if (Rollout.this.cachedOpt.nonEmpty) {
            // no need
          } else {
            play()
          }
          enableCached
        }
      }

      lazy val trajectory: Seq[Observation] = cached.trajectory
    }

    override def withCtx(v: SpookyContext): _WithCtx = _WithCtx(v)
  }

  object Rollout {

    object Cached extends Capability
    type Cached = Cached.type

    implicit class CachedRolloutView(v: Rollout <> Cached) {

      def trajectory: Seq[Observation] = v.cachedOpt.get
      // TODO: returned type will become a class
    }
  }
}

case class Trace(
    self: Repr = Nil
    // TODO: this should be gone, delegating to Same.By.Wrapper
) extends Actions
    with HasTrace { // remember trace is not a block! its the super container that cannot be wrapped

  import Trace.*

  override def trace: Trace = this
  override def children: Trace = trace

  override def toString: String = children.mkString("{ ", " -> ", " }")

  override def apply(agent: Agent): Seq[Observation] = {
    // the state of the agent is unknown, cannot cache result so far

    val result = doFetch(agent)
    result
  }

  protected[actions] def doFetch(agent: Agent, lazyExe: Boolean = false): Seq[Observation] = {

    val results: Seq[Observation] = if (this.isEmpty) {
      Nil
    } else {

      val _children: Seq[Action] =
        if (lazyExe) children.to(LazyList)
        // this is a good pattern as long as anticipated result doesn't grow too long
        else children

      _children.flatMap { action =>
        val observed: Seq[Observation] = action.apply(agent)
        agent.backtrace ++= action.skeleton

        if (action.hasOutput) {

          val spooky = agent.spooky

          observed.foreach { observed =>
            spooky.conf.auditing.apply(observed).map { doc =>
              doc.prepareSave(spooky).auditing()
            }
            // TODO: doc.content can now use the file saved for auditing
          }

          if (spooky.conf.cacheWrite) {

            val failures = observed.collect {
              case v: Observation.Failure => v
            }
            // will not cache even if there is only 1 failure
            if (failures.isEmpty) {

              observed.headOption.foreach { v =>
                val effectiveBacktrace = v.uid.backtrace
                InMemoryDocCache.put(effectiveBacktrace, observed, spooky)
                DFSDocCache.put(effectiveBacktrace, observed, spooky)
              }
            }
          }
          observed
        } else {
          assert(observed.isEmpty)
          Nil
        }
      }
    }

    results
  }

  override lazy val dryRun: DryRun = {
    val result: ArrayBuffer[Repr] = ArrayBuffer()

    for (i <- children.indices) {
      val child = children(i)
      if (child.hasOutput) {
        val backtrace: Repr = child match {
          case _: Action.Driverless => child :: Nil
          case _                    => children.slice(0, i).flatMap(_.skeleton) :+ child
        }
        result += backtrace
      }
    }

    result.map(v => v: Trace).toList
  }

  def rewriteLocally(schema: SpookySchema): TraceSet = {

    TraceSet.of(RewriteRule.Rules(localRewriteRules(schema)).rewriteAll(Seq(this))*)
  }

  // the minimal equivalent action that can be put into backtrace
  override def skeleton: Option[Trace.this.type] =
    Some(new Trace(this.childrenSkeleton).asInstanceOf[this.type])

}
