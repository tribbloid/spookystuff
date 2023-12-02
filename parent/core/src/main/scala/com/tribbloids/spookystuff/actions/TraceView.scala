package com.tribbloids.spookystuff.actions

import ai.acyclic.prover.commons.EqualBy
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.caching.{DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.doc.{Doc, Fetched}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session

import scala.collection.mutable.ArrayBuffer

object TraceView {

  def withDocs(
      children: Trace = Nil,
      keyBy: Trace => Any = identity,
      docs: Seq[Fetched] = Nil
  ): TraceView = {
    val result = apply(children, keyBy)
    result.docs = docs
    result
  }
}

case class TraceView(
    override val children: Trace = Nil,
    keyBy: Trace => Any = identity // used by custom keyBy arg in fetch and explore.
) extends Actions
    with TraceAPI
    with EqualBy { // remember trace is not a block! its the super container that cannot be wrapped

  @transient override lazy val asTrace: Trace = children

  val samenessDelegatedTo: Any = keyBy(children)

  override def toString: String = children.mkString("{ ", " -> ", " }")

  @volatile @transient private var docs: Seq[Fetched] = _
  // override, cannot be shipped, lazy evaluated TODO: not volatile?
  def docsOpt: Option[Seq[Fetched]] = Option(docs)

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

    this.docs = results

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

    val interpolatedOpt = interpolate(row, schema)
    val result = interpolatedOpt.toSeq.flatMap { v =>
      v.rewriteLocally(schema)
    }

    result
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
    def getDoc: Seq[Fetched] = TraceView.this.synchronized {
      docsOpt.getOrElse {
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
