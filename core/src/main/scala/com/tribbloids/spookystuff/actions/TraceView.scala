package com.tribbloids.spookystuff.actions

import org.slf4j.LoggerFactory
import com.tribbloids.spookystuff.row.FetchedRow
import com.tribbloids.spookystuff.doc.{Doc, Fetched, PageUtils}
import com.tribbloids.spookystuff.session.{DriverSession, NoDriverSession, Session}
import com.tribbloids.spookystuff.utils.Utils
import com.tribbloids.spookystuff.{RemoteDisabledException, dsl, Const, SpookyContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by peng on 10/25/14.
 */
case class TraceView(
                 override val children: Trace
                 ) extends Actions(children) { //remember trace is not a block! its the super container that cannot be wrapped

  //always has output (Sometimes Empty) to handle left join
  override def doInterpolate(pr: FetchedRow, spooky: SpookyContext): Option[this.type] = {
    val seq = this.doInterpolateSeq(pr, spooky)

    Some(new TraceView(seq).asInstanceOf[this.type])
  }

  override def apply(session: Session): Seq[Fetched] = {

    val results = new ArrayBuffer[Fetched]()

    this.children.foreach {
      action =>
        val result = action.apply(session)
        session.backtrace ++= action.trunk

        if (action.hasOutput) {

          results ++= result
          session.spooky.metrics.pagesFetchedFromRemote += result.count(_.isInstanceOf[Doc])

          val spooky = session.spooky

          if (spooky.conf.autoSave) result.foreach{
            case page: Doc => page.autoSave(spooky)
            case _ =>
          }
          if (spooky.conf.cacheWrite) PageUtils.autoCache(result, spooky)
        }
        else {
          assert(result.isEmpty)
        }
    }

    results
  }

  lazy val dryrun: DryRun = {
    val result: ArrayBuffer[Trace] = ArrayBuffer()

    for (i <- children.indices) {
      val selfi = children(i)
      if (selfi.hasOutput){
        val backtrace: List[Action] = selfi match {
          case dl: Driverless => selfi :: Nil
          case _ => children.slice(0, i).flatMap(_.trunk) :+ selfi
        }
        result += backtrace
      }
    }

    result.toList
  }

  //if Trace has no output, automatically append Snapshot
  //invoke before interpolation!
  def correct: Trace = {
    if (children.isEmpty) children
    else if (children.last.hasOutput) children
    else children :+ Snapshot() //Don't use singleton, otherwise will flush timestamp and name
  }

  def fetch(spooky: SpookyContext): Seq[Fetched] = {

    val results = Utils.retry (Const.remoteResourceLocalRetries){
      fetchOnce(spooky)
    }
    val numPages = results.count(_.isInstanceOf[Doc])
    spooky.metrics.pagesFetched += numPages

    results
  }

  def fetchOnce(spooky: SpookyContext): Seq[Fetched] = {

    if (!this.hasOutput) return Nil

    val pagesFromCache = if (!spooky.conf.cacheRead) Seq(null)
    else dryrun.map(
      dry =>
        PageUtils.autoRestore(dry, spooky)
    )

    if (!pagesFromCache.contains(null)){

      spooky.metrics.fetchFromCacheSuccess += 1

      val results = pagesFromCache.flatten
      spooky.metrics.pagesFetchedFromCache += results.count(_.isInstanceOf[Doc])
      this.children.foreach{
        action =>
          LoggerFactory.getLogger(this.getClass).info(s"(cached)+> ${action.toString}")
      }

      results
    }
    else {

      spooky.metrics.fetchFromCacheFailure += 1

      if (!spooky.conf.remote) throw new RemoteDisabledException(
        "Resource is not cached and not enabled to be fetched remotely, " +
          "the later can be enabled by setting SpookyContext.conf.remote=true"
      )

      val session = if (children.count(_.needDriver) == 0) new NoDriverSession(spooky)
      else new DriverSession(spooky)
      try {
        val result = this.apply(session)
        spooky.metrics.fetchFromRemoteSuccess += 1
        result
      }
      catch {
        case e: Throwable =>
          spooky.metrics.fetchFromRemoteFailure += 1
          throw e
      }
      finally {
        session.close()
      }
    }
  }

  //the minimal equivalent action that can be put into backtrace
  override def trunk = Some(new TraceView(this.trunkSeq).asInstanceOf[this.type])
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
    trace => others.map {
      case otherAction: Action => trace :+ otherAction
      case otherTrace: Trace => trace ++ otherTrace
    }
  )

  def ||(other: TraversableOnce[Trace]): Set[Trace] = self ++ other

  def correct: Set[Trace] = self.map(_.correct)

  def interpolate(row: FetchedRow, context: SpookyContext): Set[Trace] = self.flatMap(_.interpolate(row, context: SpookyContext).map(_.children))

  def outputNames: Set[String] = self.map(_.outputNames).reduce(_ ++ _)
}