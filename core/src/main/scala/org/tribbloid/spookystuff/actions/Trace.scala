package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.entity.{Page, PageRow}
import org.tribbloid.spookystuff.factory.PageBuilder
import org.tribbloid.spookystuff.utils.{Utils, Const}

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

  //TODO: migrate all lazy-evaluation and cache here, PageBuilder should not handle this
  override def apply(session: PageBuilder): Seq[Page] = {

    session ++= self
    session.pages
  }

  //invoke before interpolation!
  def autoSnapshot: Trace = {
    if (this.mayExport && self.nonEmpty) this
    else Trace(self :+ Snapshot()) //Don't use singleton, otherwise will flush timestamp and name
  }

  def resolve(spooky: SpookyContext): Seq[Page] = {

    Utils.retry (Const.remoteResourceInPartitionRetry){
      resolvePlain(spooky)
    }
  }

  //no retry
  def resolvePlain(spooky: SpookyContext): Seq[Page] = {

    //    val results = ArrayBuffer[Page]()

    val pb = new PageBuilder(spooky)
    spooky.accumulables.driverInitialized += 1

    try {
      this.apply(pb)
    }
    finally {
      pb.close()

      spooky.accumulables.driverClosed += 1
    }
  }

  //the minimal equivalent action that can be put into backtrace
  override def trunk = Some(Trace(this.trunkSeq).asInstanceOf[this.type])
}

//TODO: verify this! document is really scarce
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
final class TraceSetFunctions(self: Set[Trace]) {

  //one-to-one
  def +>(another: Action): Set[Trace] = self.map(chain => Trace(chain.self :+ another))
  def +>(others: TraversableOnce[Action]): Set[Trace] = self.map(chain => Trace(chain.self ++ others))

  //one-to-one truncate longer
  def +>(others: Iterable[Trace]): Set[Trace] = self.zip(others).map(tuple => Trace(tuple._1.self ++ tuple._2.self))

  //one-to-many

  def *>[T: ClassTag](others: TraversableOnce[T]): Set[Trace] = self.flatMap(
    trace => others.map {
      case action: Action => Trace(trace.self :+ action)
      case other: Trace => Trace(trace.self ++ other.self)
    }
  )

  def ||(other: TraversableOnce[Trace]): Set[Trace] = self ++ other

  def autoSnapshot: Set[Trace] = self.map(_.autoSnapshot)

  def interpolate(pr: PageRow): Set[Trace] = self.flatMap(_.interpolate(pr))

  def outputs: Set[String] = self.map(_.outputs).reduce(_ ++ _)
}