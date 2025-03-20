package com.tribbloids.spookystuff.actions

import ai.acyclic.prover.commons.cap.Capability
import ai.acyclic.prover.commons.cap.Capability.<>
import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import com.tribbloids.spookystuff.{Const, QueryException, SpookyContext}
import com.tribbloids.spookystuff.actions.HasTrace.MayChangeState
import com.tribbloids.spookystuff.actions.Foundation.HasTraceSet
import com.tribbloids.spookystuff.actions.Trace.Repr
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.caching.{CacheKey, DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.commons.CommonUtils
import com.tribbloids.spookystuff.doc.{Doc, Observation}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

object Trace {

  private type Repr = Seq[Action]

  implicit def unbox(v: Trace): Repr = v.self
  implicit def box(v: Repr): Trace = Trace(v)

  type DryRun = List[Trace]

  def of(vs: Action*): Trace = Trace(vs.toList)

  /**
    * read [[ai.acyclic.prover.commons.__OperatorPrecedence]] when defining new operators
    * @param traceSet
    *   no duplicated Trace
    */
  implicit class _setView(val traceSet: Set[Trace]) extends HasTraceSet {

    def outputNames: Set[String] = traceSet.map(_.exportNames).reduce(_ ++ _)

    //      def avoidEmpty: NonEmpty = {
    //        val result = if (traces.isEmpty) {
    //          FetchImpl(Set(Trace.NoOp))
    //        } else {
    //          this
    //        }
    //        result.asInstanceOf[NonEmpty]
    //      }
  }

  case class Rollout(trace: Trace) extends SpookyContext.Contextual {
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
    private var _cached: Seq[Observation] = {
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

    implicit class _WithCtx(spooky: SpookyContext) extends NOTSerializable {

      def play(): Seq[Observation] = {
        val result = trace.fetch(spooky)
        Rollout.this._cached = result
        result
      }

      lazy val cached: Rollout <> Cached = {

        Trace.this.synchronized {

          if (Rollout.this.cachedOpt.nonEmpty) {
            // no need

//            print_@(s"cached ${Rollout.this}, no need to play")
          } else {

//            print_@(s"playing ${Rollout.this}")
            play()
          }
          enableCached
        }
      }

      lazy val trajectory: Seq[Observation] = cached.trajectory
    }
  }

  object Rollout {

    object Cached extends Capability
    type Cached = Cached.type

    implicit def fromTrace(v: Trace): Rollout = Rollout(v)

    implicit class _cachedView(v: Rollout <> Cached) {

      def trajectory: Seq[Observation] = v.cachedOpt.get
      // TODO: returned type will become a class
    }
  }
}

case class Trace(
    self: Repr = Nil
    // TODO: this should be gone, delegating to Same.By.Wrapper
) extends Actions
    with MayChangeState { // remember trace is not a block! its the super container that cannot be wrapped

  import Trace.*

  override def trace: Trace = this

  override def toString: String = trace.mkString("{ ", " -> ", " }")

//  final def trace_anonymous: Trace = {
//
//    val transformed = trace.map {
//
//      case Named(v, _) => v
//      case v           => v
//    }
//
//    transformed
//  }

  final def cacheKey: CacheKey = {

    CacheKey.NormalFormKey(trace)
  }

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
        if (lazyExe) trace.to(LazyList)
        // this is a good pattern as long as anticipated result doesn't grow too long
        else trace

      _children.flatMap { action =>
        val observed: Seq[Observation] = action.apply(agent)
        agent.backtrace ++= action.stateChangeOnly

        if (action.hasExport) {

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

  def fetch(spooky: SpookyContext): Seq[Observation] = {

    val results = CommonUtils.retry(Const.remoteResourceLocalRetries) {
      fetchOnce(spooky)
    }
    val numPages = results.count(_.isInstanceOf[Doc])
    spooky.metrics.pagesFetched += numPages

    results
  }

  def fetchOnce(spooky: SpookyContext): Seq[Observation] = {

    if (!this.hasExport) return Nil

    val pagesFromCache: Seq[Option[Seq[Observation]]] =
      if (!spooky.conf.cacheRead) Seq(None)
      else
        dryRun.map { dry =>
          val view = Trace(dry)
          InMemoryDocCache
            .get(view, spooky)
            .orElse {
              DFSDocCache.get(view, spooky)
            }
        }

    if (!pagesFromCache.contains(None)) {
      // cache incomplete, dryrun yields some backtraces that cannot be found, a new fetch from scratch is necessary
      spooky.metrics.fetchFromCacheSuccess += 1

      val results = pagesFromCache.map(v => v.get).flatten
      spooky.metrics.pagesFetchedFromCache += results.count(_.isInstanceOf[Doc])
      this.trace.foreach { action =>
        LoggerFactory.getLogger(this.getClass).info(s"(cached)+> ${action.toString}")
      }

      results
    } else {
      spooky.metrics.fetchFromCacheFailure += 1

      if (!spooky.conf.remote)
        throw new QueryException(
          "Resource is not cached and not allowed to be fetched remotely, " +
            "the later can be enabled by setting SpookyContext.conf.remote=true"
        )
      spooky.withSession { session =>
        try {
          val result = this.apply(session)
          spooky.metrics.fetchFromRemoteSuccess += 1
          result
        } catch {
          case e: Exception =>
            spooky.metrics.fetchFromRemoteFailure += 1
            session.Drivers.releaseAll()
            throw e
        }
      }
    }
  }

  lazy val dryRun: DryRun = {
    val result: ArrayBuffer[Repr] = ArrayBuffer()

    for ((child, i) <- trace.zipWithIndex) yield {

      if (child.hasExport) {
        val backtrace: Repr = child match {
          case _: Action.Driverless => child :: Nil
          case _ =>
            val preceding = trace.slice(0, i)
            preceding.flatMap(_.stateChangeOnly) :+ child
        }
        result += backtrace
      }
    }

    result.map(v => v: Trace).toList
  }

  // the minimal equivalent action that can be put into backtrace
  override def stateChangeOnly: Trace = {

    self.flatMap {

      case child: Action with MayChangeState =>
        val result = child.stateChangeOnly.trace

        result
      case _ => Nil
    }
  }
}
