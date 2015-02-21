package org.tribbloid.spookystuff.actions

import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff.{dsl, Const}
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.pages.{NoPage, Page, PageLike}
import org.tribbloid.spookystuff.session.Session

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
 * Only for complex workflow control,
 * each defines a nested/non-linear subroutine that may or may not be executed
 * once or multiple times depending on situations.
 */
abstract class Block(override val self: Seq[Action]) extends Actions(self) with Named {

  //  assert(self.nonEmpty)

  override def as(name: Symbol) = {
    super.as(name)

    self.foreach{
      case n: Named => n.as(name)
      case _ =>
    }

    this
  }

  final override def doExe(session: Session): Seq[PageLike] = {

    val pages = this.doExeNoUID(session)

    val backtrace = Trace(session.backtrace :+ this)
    val result = pages.zipWithIndex.map {
      tuple => {
        val page = tuple._1

        page.copy(uid = page.uid.copy(backtrace = backtrace, blockIndex = tuple._2, blockTotal = pages.size))
      }
    }
    if (result.isEmpty && this.hasExport) Seq(NoPage(backtrace))
    else result
  }

  def doExeNoUID(session: Session): Seq[Page]
}

final case class Try(override val self: Seq[Action]) extends Block(self) {

  override def trunk = Some(Try(this.trunkSeq).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Page] = {

    val pages = new ArrayBuffer[Page]()

    try {
      for (action <- self) {
        pages ++= action.exe(session).flatMap{
          case page: Page => Some(page)
          case noPage: NoPage => None
        }
      }
    }
    catch {
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).info("Aborted on exception: " + e)
    }

    pages
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] ={
    val seq = this.doInterpolateSeq(pageRow)
    if (seq.isEmpty) None
    else Some(this.copy(self = seq).asInstanceOf[this.type])
  }
}

object Try {

  def apply(
             trace: Set[Trace]
             ): Try = {
    assert(trace.size == 1)

    Try(trace.head.self)
  }
}

/**
 * Contains several sub-actions that are iterated for multiple times
 * Will iterate until max iteration is reached or execution is impossible (sub-action throws an exception)
 * @param limit max iteration, default to Const.fetchLimit
 * @param self a list of actions being iterated through
 */
final case class Loop(
                       override val self: Seq[Action],
                       limit: Int
                       ) extends Block(self) {

  assert(limit>0)

  override def trunk = Some(this.copy(self = this.trunkSeq).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Page] = {

    val pages = new ArrayBuffer[Page]()

    try {
      for (i <- 0 until limit) {

        for (action <- self) {
          pages ++= action.exe(session).flatMap{
            case page: Page => Some(page)
            case noPage: NoPage => None
          }
        }
      }
    }
    catch {
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).info("Aborted on exception: " + e)
    }

    pages
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] ={
    val seq = this.doInterpolateSeq(pageRow)
    if (seq.isEmpty) None
    else Some(this.copy(self = seq).asInstanceOf[this.type])
  }
}

object Loop {

  def apply(
             trace: Set[Trace],
             limit: Int = Const.maxLoop
             ): Loop = {
    assert(trace.size == 1)

    Loop(trace.head.self, limit)
  }
}

//case class cannot be overridden
//syntax sugar for loop-click-wait
object LoadMore {

  import dsl._

  def apply(
             selector: String,
             limit: Int = Const.maxLoop,
             delay: Duration = Const.delayMin
             ): Loop =
    Loop(
      Delay(delay)
        +> WaitForDocumentReady
        +> Click(selector),
      limit
    )
}

object Paginate {

  import dsl._

  def apply(
             selector: String,
             limit: Int = Const.maxLoop,
             delay: Duration = Const.delayMin
             ): Loop = {
    Loop(
      Delay(delay)
        +> WaitForDocumentReady
        +> Snapshot()
        +> Click(selector),
      limit
    )
  }
}

final case class If(
                     condition: Page => Boolean,
                     ifTrue: Seq[Action] = Seq(),
                     ifFalse: Seq[Action] = Seq()
                     ) extends Block(ifTrue ++ ifFalse) {

  override def trunk = Some(this.copy(ifTrue = ifTrue.flatMap(_.trunk), ifFalse = ifFalse.flatMap(_.trunk)).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Page] = {

    val current = DefaultSnapshot.exe(session)(0).asInstanceOf[Page]

    val pages = new ArrayBuffer[Page]()
    if (condition(current)) {
      for (action <- ifTrue) {
        pages ++= action.exe(session).flatMap{
          case page: Page => Some(page)
          case noPage: NoPage => None
        }
      }
    }
    else {
      for (action <- ifFalse) {
        pages ++= action.exe(session).flatMap{
          case page: Page => Some(page)
          case noPage: NoPage => None
        }
      }
    }

    pages
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] ={
    val ifTrueInterpolated = Actions.doInterppolateSeq(ifTrue, pageRow)
    val ifFalseInterpolated = Actions.doInterppolateSeq(ifFalse, pageRow)
    val result = this.copy(ifTrue = ifTrueInterpolated, ifFalse = ifFalseInterpolated).asInstanceOf[this.type]
    Some(result)
  }
}