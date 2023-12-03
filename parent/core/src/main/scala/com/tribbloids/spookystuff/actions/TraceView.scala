package com.tribbloids.spookystuff.actions

import ai.acyclic.prover.commons.EqualBy
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.caching.{DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.doc.{Doc, Fetched}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session

import scala.collection.mutable.ArrayBuffer

object TraceView {}

case class TraceView(
    override val children: Trace = Nil,
    keyBy: Trace => Any = identity // used by custom keyBy arg in fetch and explore.
) extends Actions
    with TraceAPI
    with EqualBy { // remember trace is not a block! its the super container that cannot be wrapped

  @transient override lazy val asTrace: Trace = children

  val samenessDelegatedTo: Any = keyBy(children)

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
    val result: ArrayBuffer[Trace] = ArrayBuffer()

    for (i <- children.indices) {
      val child = children(i)
      if (child.hasOutput) {
        val backtrace: Trace = child match {
          case _: Driverless => child :: Nil
          case _             => children.slice(0, i).flatMap(_.skeleton) :+ child
        }
        result += backtrace
      }
    }

    result.toList
  }

  // always has output (Sometimes Empty) to handle left join
  override def doInterpolate(row: FetchedRow, schema: SpookySchema): Option[this.type] = {
    val seq = this.doInterpolateSeq(row, schema)

    Some(new TraceView(seq).asInstanceOf[this.type])
  }

  override def globalRewriteRules(schema: SpookySchema): List[RewriteRule[TraceView]] =
    children.flatMap(_.globalRewriteRules(schema)).distinct

  def rewriteGlobally(schema: SpookySchema): Seq[TraceView] = {
    RewriteRule.Rules(globalRewriteRules(schema)).rewriteAll(Seq(this))
  }

  def interpolateAndRewriteLocally(row: FetchedRow, schema: SpookySchema): Seq[TraceView] = {
    // TODO: isolate interploation into an independent rewriter?
    val interpolatedOpt: Option[Trace] = interpolate(row, schema)
      .map(_.children)
    interpolatedOpt.toSeq.flatMap { v =>
      TraceView(v).rewriteLocally(schema)
    }
  }

  def rewriteLocally(schema: SpookySchema): Seq[TraceView] = {

    RewriteRule.Rules(localRewriteRules(schema)).rewriteAll(Seq(this))
  }

  // the minimal equivalent action that can be put into backtrace
  override def skeleton: Option[TraceView.this.type] =
    Some(new TraceView(this.childrenSkeleton).asInstanceOf[this.type])

  class WithSpooky(spooky: SpookyContext) {

    // fetched may yield very large documents and should only be
    // loaded lazily and not shuffled or persisted (unless in-memory)
    def observations: Seq[Fetched] = TraceView.this.synchronized {
      cachedOpt.getOrElse {
        fetch
      }
    }

    def fetch: Seq[Fetched] = {
      val docs = TraceView.this.fetch(spooky)
      docs
    }
  }

  def keyBy[T](v: Trace => T): TraceView = this.copy(keyBy = v)
}
