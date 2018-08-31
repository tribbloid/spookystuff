package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.caching.{DFSDocCache, InMemoryDocCache}
import com.tribbloids.spookystuff.doc.{Doc, DocOption}
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.IDMixin
import com.tribbloids.spookystuff.{dsl, SpookyContext}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

object TraceView {

  implicit def fromTrace(trace: Trace): TraceView = TraceView(trace)

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
) extends Actions(children)
    with IDMixin { //remember trace is not a block! its the super container that cannot be wrapped

  val _id: Any = keyBy(children)

  override def toString = children.mkString("{ ", " -> ", " }")

  @volatile @transient private var docs: Seq[DocOption] = _ //override, cannot be shipped, lazy evaluated TODO: not volatile?
  def docsOpt = Option(docs)

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

  override lazy val dryrun: DryRun = {
    val result: ArrayBuffer[Trace] = ArrayBuffer()

    for (i <- children.indices) {
      val child = children(i)
      if (child.hasOutput) {
        val backtrace: Trace = child match {
          case dl: Driverless => child :: Nil
          case _              => children.slice(0, i).flatMap(_.skeleton) :+ child
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

  override lazy val globalRewriters = children.flatMap(_.globalRewriters).distinct

  def rewriteGlobally(schema: SpookySchema): Trace = {
    globalRewriters.foldLeft(children) { (trace, rewriter) =>
      rewriter.rewrite(trace, schema)
    }
  }

  def interpolateAndRewriteLocally(row: FetchedRow, schema: SpookySchema): Option[Trace] = {
    //TODO: isolate interploation into an independent rewriter?
    val interpolatedOpt: Option[Trace] = interpolate(row, schema)
      .map(_.children)
    interpolatedOpt.flatMap { v =>
      TraceView(v).rewriteLocally(schema)
    }
  }

  def rewriteLocally(schema: SpookySchema): Option[Trace] = {
    val interpolatedOpt: Option[Trace] = Some(this.children)
    val result = localRewriters.foldLeft(interpolatedOpt) { (opt, rewriter) =>
      opt.flatMap { trace =>
        rewriter.rewrite(trace, schema)
      }
    }
    result
  }

  //the minimal equivalent action that can be put into backtrace
  override def skeleton = Some(new TraceView(this.trunkSeq).asInstanceOf[this.type])

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

object TraceSetView {

  implicit def fromTrace(traces: Trace): TraceSetView = TraceSetView(Set(traces))

  implicit def fromAction(action: Action): TraceSetView = TraceSetView(Set(List(action)))
}

//The precedence of an inﬁx operator is determined by the operator’s ﬁrst character.
//Characters are listed below in increasing order of precedence, with characters on
//the same line having the same precedence.
//(all letters)
//|
//^
//&
//= !.................................................(new doc)
//< >
//= !.................................................(old doc)
//:
//+ -
//* / %
//(all other special characters)
//now using immutable pattern to increase maintainability
//put all narrow transformation closures here
//TODO: this list is incomplete, some operators, e.g. # are missing
final case class TraceSetView(self: Set[Trace]) {

  import dsl._

  //one-to-one
  def +>(another: Action): Set[Trace] = self.map(trace => trace :+ another)
  def +>(others: TraversableOnce[Action]): Set[Trace] = self.map(trace => trace ++ others)

  //one-to-one truncate longer
  def +>(others: Iterable[Trace]): Set[Trace] = self.zip(others).map(tuple => tuple._1 ++ tuple._2)

  //one-to-many

  def *>[T: ClassTag](others: TraversableOnce[T]): Set[Trace] = self.flatMap(
    trace =>
      others.map {
        case otherAction: Action => trace :+ otherAction
        case otherList: List[_] =>
          trace ++ otherList.collect {
            case v: Action => v
          }
    }
  )

  def ||(other: TraversableOnce[Trace]): Set[Trace] = self ++ other

  def rewriteGlobally(schema: SpookySchema): Set[Trace] = self.map(_.rewriteGlobally(schema))

  //  def interpolate(row: FetchedRow, schema: DataRowSchema): Set[Trace] =
  //    self.flatMap(_.interpolate(row, schema: DataRowSchema).map(_.children))

  def outputNames: Set[String] = self.map(_.outputNames).reduce(_ ++ _)
}
