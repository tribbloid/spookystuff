package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.caching.{DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.doc.{Doc, DocOption}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.IDMixin

import scala.collection.mutable.ArrayBuffer

object TraceView {

  def withDocs(
      children: Trace = Nil,
      keyBy: Trace => Any = identity,
      docs: Seq[DocOption] = Nil
  ): TraceView = {
    val result = apply(children, keyBy)
    result.docs = docs
    result
  }
}

case class TraceView(
    override val children: Trace = Nil,
    keyBy: Trace => Any = identity //used by custom keyBy arg in fetch and explore.
) extends Actions
    with TraceAPI
    with IDMixin { //remember trace is not a block! its the super container that cannot be wrapped

  @transient override lazy val asTrace: Trace = children

  val _id: Any = keyBy(children)

  override def toString: String = children.mkString("{ ", " -> ", " }")

  @volatile @transient private var docs: Seq[DocOption] = _
  //override, cannot be shipped, lazy evaluated TODO: not volatile?
  def docsOpt: Option[Seq[DocOption]] = Option(docs)

  override def apply(session: Session): Seq[DocOption] = {

    _apply(session)
  }

  protected[actions] def _apply(session: Session, lazyStream: Boolean = false): Seq[DocOption] = {

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

  //always has output (Sometimes Empty) to handle left join
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
    //TODO: isolate interploation into an independent rewriter?
    val interpolatedOpt: Option[Trace] = interpolate(row, schema)
      .map(_.children)
    interpolatedOpt.toSeq.flatMap { v =>
      TraceView(v).rewriteLocally(schema)
    }
  }

  def rewriteLocally(schema: SpookySchema): Seq[TraceView] = {

    RewriteRule.Rules(localRewriteRules(schema)).rewriteAll(Seq(this))
  }

  //the minimal equivalent action that can be put into backtrace
  override def skeleton: Option[TraceView.this.type] =
    Some(new TraceView(this.childrenSkeleton).asInstanceOf[this.type])

  class WithSpooky(spooky: SpookyContext) {

    //fetched may yield very large documents and should only be
    // loaded lazily and not shuffled or persisted (unless in-memory)
    def getDoc: Seq[DocOption] = TraceView.this.synchronized {
      docsOpt.getOrElse {
        fetch
      }
    }

    def fetch: Seq[DocOption] = {
      val docs = TraceView.this.fetch(spooky)
      docs
    }
  }

  def keyBy[T](v: Trace => T): TraceView = this.copy(keyBy = v)
}
