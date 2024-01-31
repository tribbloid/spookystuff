package com.tribbloids.spookystuff.actions

import ai.acyclic.prover.commons.util.Capabilities
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace.Internal
import com.tribbloids.spookystuff.caching.{DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.doc.{Doc, Observation}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

object Trace {

  private type Internal = List[Action]

  implicit def unbox(v: Trace): Internal = v.self
  implicit def box(v: Internal): Trace = Trace(v)

  type DryRun = List[Trace]

  def of(vs: Action*): Trace = Trace(vs.toList)

  case class Rollout(trace: Trace) extends HasTrace with Rollout.NoCap with SpookyContext.CanRunWith {
    // unlike trace, it is always executed by the agent from scratch
    // thus, execution result can be cached, as replaying it will most likely have the same result (if the trace is deterministic)

    import Rollout._

    /**
      * deliberately NON-transient to be included in serialized from
      *
      * but can be manually discarded by calling [[unCache]] to make serialization even faster
      *
      * assuming that most [[Observation]]s will be auto-saved into DFS, their serialized form can exclude contents and
      * thus be very small, making such manual discarding an optimisation with diminishing return
      */
    @volatile private var _cached: Seq[Observation] = _

    def cachedOpt: Option[Seq[Observation]] = Option(_cached)

    def enableCached: Rollout with Cached = this.asInstanceOf[Rollout with Cached]
    def disableCached: Rollout = this.asInstanceOf[Rollout]

    def cache(vs: Seq[Observation]): Rollout with Cached = {
      this._cached = vs
      this.enableCached
    }

    def unCache: Rollout = {
      this._cached = null
      this.disableCached
    }

    case class _WithCtx(spooky: SpookyContext) extends NOTSerializable {

      def play(): Seq[Observation] = {
        val result = trace.fetch(spooky)
        Rollout.this._cached = result
        result
      }

      lazy val cached: Rollout with Cached = {

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
  }

  object Rollout extends Capabilities {

    trait Cached extends Cap

    implicit class CachedRolloutView(v: Rollout with Cached) {

      def trajectory: Seq[Observation] = v.cachedOpt.get
      // TODO: returned type will become a class
    }
  }

  lazy val NoOp: Rollout with Rollout.Cached = Rollout(Trace()).cache(Nil)

}

case class Trace(
    self: Internal = Nil
    // TODO: this should be gone, delegating to Same.By.Wrapper
) extends Actions
    with HasTrace { // remember trace is not a block! its the super container that cannot be wrapped

  import Trace._

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
        val actionResult = action.apply(agent)
        agent.backtrace ++= action.skeleton

        if (action.hasOutput) {

          val spooky = agent.spooky

          if (spooky.spookyConf.autoSave) actionResult.foreach {
            case page: Doc => page.autoSave(spooky)
            case _         =>
          }
          if (spooky.spookyConf.cacheWrite) {
            val effectiveBacktrace = actionResult.head.uid.backtrace
            InMemoryDocCache.put(effectiveBacktrace, actionResult, spooky)
            DFSDocCache.put(effectiveBacktrace, actionResult, spooky)
          }
          actionResult
        } else {
          assert(actionResult.isEmpty)
          Nil
        }
      }
    }

    results
  }

  override lazy val dryRun: DryRun = {
    val result: ArrayBuffer[Internal] = ArrayBuffer()

    for (i <- children.indices) {
      val child = children(i)
      if (child.hasOutput) {
        val backtrace: Internal = child match {
          case _: Driverless => child :: Nil
          case _             => children.slice(0, i).flatMap(_.skeleton) :+ child
        }
        result += backtrace
      }
    }

    result.map(v => v: Trace).toList
  }

  // always has output (Sometimes Empty) to handle left join
  override def doInterpolate(row: FetchedRow, schema: SpookySchema): Option[this.type] = {
    val opt = this.doInterpolateSeq(row, schema)

    opt.map { seq =>
      new Trace(seq).asInstanceOf[this.type]
    }
  }

  override def globalRewriteRules(schema: SpookySchema): List[RewriteRule[Trace]] =
    children.flatMap(_.globalRewriteRules(schema)).distinct

  def rewriteGlobally(schema: SpookySchema): TraceSet = {
    val result = RewriteRule.Rules(globalRewriteRules(schema)).rewriteAll(Seq(this))
    TraceSet.of(result: _*)
  }

  /**
    * @param row
    *   with regard to
    * @param schema
    *   with regard to
    * @return
    *
    * will never return collection that contains NoOp
    *
    * may return empty TraceSet Which means interpolation has failed, leading to no executable trace
    */
  def interpolateAndRewrite(row: FetchedRow, schema: SpookySchema): TraceSet = {

    val interpolated = interpolate(row, schema)
    val rewritten: Seq[Trace] = interpolated.toSeq.flatMap { v =>
      v.rewriteLocally(schema)
    }

    val result = TraceSet.of(rewritten.filter(_.nonEmpty): _*)
    result
  }

  def rewriteLocally(schema: SpookySchema): TraceSet = {

    TraceSet.of(RewriteRule.Rules(localRewriteRules(schema)).rewriteAll(Seq(this)): _*)
  }

  // the minimal equivalent action that can be put into backtrace
  override def skeleton: Option[Trace.this.type] =
    Some(new Trace(this.childrenSkeleton).asInstanceOf[this.type])

}
