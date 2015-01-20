package org.tribbloid.spookystuff.actions

import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.pages.{Page, PageLike, PageUtils}
import org.tribbloid.spookystuff.session.{DriverSession, NoDriverSession, Session}
import org.tribbloid.spookystuff.utils.Utils
import org.tribbloid.spookystuff.{Const, SpookyContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by peng on 10/25/14.
 */
final case class Trace(
                        override val self: Seq[Action]
                        ) extends Actions(self) { //remember chain is not a block! its the super container that cannot be wrapped

  //always has output to handle left join
  override def doInterpolate(pr: PageRow): Option[this.type] = {
    val seq = this.doInterpolateSeq(pr)

    Some(Trace(seq).asInstanceOf[this.type])
  }

  override def apply(session: Session): Seq[PageLike] = {

    val results = new ArrayBuffer[PageLike]()

    this.self.foreach {
      action =>
        val result = action.apply(session)
        session.backtrace ++= action.trunk

        if (action.hasExport) {
          assert(result.nonEmpty) //should always happen after introduction of NoPage

          results ++= result
          session.spooky.metrics.pagesFetchedFromWeb += result.count(_.isInstanceOf[Page])

          val spooky = session.spooky

          if (spooky.autoSave) result.foreach{
            case page: Page => page.autoSave(spooky)
            case _ =>
          }
          if (spooky.cacheWrite) PageUtils.autoCache(result, spooky)
        }
        else {
          assert(result.isEmpty)
        }
    }

    results
  }

  lazy val dryrun: Seq[Trace] = {
    val result: ArrayBuffer[Trace] = ArrayBuffer()

    for (i <- 0 until self.size) {
      if (self(i).hasExport){
        val backtrace = Trace(self.slice(0, i).flatMap(_.trunk) :+ self(i))
        result += backtrace
      }
    }

    result
  }

  //invoke before interpolation!
  def autoSnapshot: Trace = {
    if (this.hasExport && self.nonEmpty) this
    else Trace(self :+ Snapshot()) //Don't use singleton, otherwise will flush timestamp and name
  }

  def resolve(spooky: SpookyContext): Seq[PageLike] = {

    val results = Utils.retry (Const.remoteResourceLocalRetry){
      resolvePlain(spooky)
    }
    val numPages = results.count(_.isInstanceOf[Page])
    spooky.metrics.pagesFetched += numPages
    results
  }

  def resolvePlain(spooky: SpookyContext): Seq[PageLike] = {

    if (!this.hasExport) return Seq()

    val pagesFromCache = dryrun.map(dry => PageUtils.autoRestoreLatest(dry, spooky))

    if (!pagesFromCache.contains(null)){
      val results = pagesFromCache.flatten
      spooky.metrics.pagesFetchedFromCache += results.count(_.isInstanceOf[Page])
      LoggerFactory.getLogger(this.getClass).info("cached page(s) found, won't go online")
      results
    }
    else {
      val session = if (self.count(_.isInstanceOf[Driverless]) == self.size) new NoDriverSession(spooky)
      else new DriverSession(spooky)
      try {
        this.apply(session)
      }
      finally {
        session.close()
      }
    }
  }

  //the minimal equivalent action that can be put into backtrace
  override def trunk = Some(Trace(this.trunkSeq).asInstanceOf[this.type])
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
final class TraceSetView(self: Set[Trace]) {

  //one-to-one
  def +>(another: Action): Set[Trace] = self.map(chain => Trace(chain.self :+ another))
  def +>(others: TraversableOnce[Action]): Set[Trace] = self.map(chain => Trace(chain.self ++ others))

  //one-to-one truncate longer
  def +>(others: Iterable[Trace]): Set[Trace] = self.zip(others).map(tuple => Trace(tuple._1.self ++ tuple._2.self))

  //one-to-many

  def *>[T: ClassTag](others: TraversableOnce[T]): Set[Trace] = self.flatMap(
    trace => others.map {
      case otherAction: Action => Trace(trace.self :+ otherAction)
      case otherTrace: Trace => Trace(trace.self ++ otherTrace.self)
    }
  )

  def ||(other: TraversableOnce[Trace]): Set[Trace] = self ++ other

  def autoSnapshot: Set[Trace] = self.map(_.autoSnapshot)

  def interpolate(pr: PageRow): Set[Trace] = self.flatMap(_.interpolate(pr))

  def outputNames: Set[String] = self.map(_.outputNames).reduce(_ ++ _)
}