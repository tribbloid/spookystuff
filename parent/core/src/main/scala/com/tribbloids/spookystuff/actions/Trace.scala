package com.tribbloids.spookystuff.actions

import ai.acyclic.prover.commons.EqualBy
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace.{DryRun, Internal}
import com.tribbloids.spookystuff.caching.{DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.doc.{Doc, Fetched}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

object Trace {

  private type Internal = List[Action]

  implicit def unbox(v: Trace): Internal = v.self
  implicit def box(v: Internal): Trace = Trace(v)

  type DryRun = List[Trace]

  object NoOp extends Trace()

  def of(vs: Action*): Trace = Trace(vs.toList)
}

case class Trace(
    self: Internal = Nil,
    samenessOvrd: Option[Any] = None // used by custom keyBy arg in fetch and explore.
) extends Actions
    with HasTrace
    with EqualBy { // remember trace is not a block! its the super container that cannot be wrapped

  override def children: Trace = this
  @transient override lazy val asTrace: Trace = children

  @transient lazy val samenessDelegatedTo: Any = samenessOvrd.getOrElse(self)

  override def toString: String = children.mkString("{ ", " -> ", " }")

  @volatile @transient private var _cached: Seq[Fetched] = _
  // override, cannot be shipped, lazy evaluated TODO: not volatile?
  def cachedOpt: Option[Seq[Fetched]] = Option(_cached)

  def setCache(vs: Seq[Fetched]): this.type = {
    this._cached = vs
    this
  }

  override def apply(session: Session): Seq[Fetched] = {

    _apply(session)
  }

  protected[actions] def _apply(session: Session, lazyStream: Boolean = false): Seq[Fetched] = {

    val _children: Seq[Action] =
      if (lazyStream) children.toStream
      // this is a good pattern as long as anticipated result doesn't grow too long
      else children

    val results = _children.flatMap { action =>
      val actionResult = action.apply(session)
      session.backtrace ++= action.skeleton

      if (action.hasOutput) {

        val spooky = session.spooky

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

    setCache(results)

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

  class WithSpooky(spooky: SpookyContext) {

    // fetched may yield very large documents and should only be
    // loaded lazily and not shuffled or persisted (unless in-memory)
    def observations: Seq[Fetched] = Trace.this.synchronized {
      cachedOpt.getOrElse {
        fetch
      }
    }

    def fetch: Seq[Fetched] = {
      val docs = Trace.this.fetch(spooky)
      docs
    }
  }

  def setSamenessFn[T](fn: Internal => T): Trace = this.copy(samenessOvrd = Option(fn(this.self)))
}
