package com.tribbloids.spookystuff.actions

import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory
import com.tribbloids.spookystuff.expressions.Expression
import com.tribbloids.spookystuff.{TryException, dsl, Const}
import com.tribbloids.spookystuff.entity.PageRow
import com.tribbloids.spookystuff.pages.{NoPage, Page, PageLike}
import com.tribbloids.spookystuff.session.Session

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
 * Only for complex workflow control,
 * each defines a nested/non-linear subroutine that may or may not be executed
 * once or multiple times depending on situations.
 */
abstract class Block(override val self: Seq[Action]) extends Actions(self) with Named with Wayback {

  //  assert(self.nonEmpty)

  override def wayback: Expression[Long] = self.flatMap {
    case w: Wayback => Some(w)
    case _ => None
  }.lastOption.map {
    _.wayback
  }.orNull

  override def as(name: Symbol) = {
    super.as(name)

    self.foreach{
      case n: Named => n.as(name)
      case _ =>
    }

    this
  }

  def cacheEmptyOutput: Boolean = true

  override def needDriver = self.map(_.needDriver).reduce(_ || _)

  final override def doExe(session: Session): Seq[PageLike] = {

    val pages = this.doExeNoUID(session)

    val backtrace = session.backtrace :+ this
    val result = pages.zipWithIndex.map {
      tuple => {
        val page = tuple._1

        page.copy(uid = page.uid.copy(backtrace = backtrace, blockIndex = tuple._2, blockSize = pages.size))
      }
    }
    if (result.isEmpty && this.hasOutput) Seq(NoPage(backtrace, cacheable = this.cacheEmptyOutput))
    else result
  }

  def doExeNoUID(session: Session): Seq[Page]
}

object Try {

  def apply(
             trace: Set[Trace],
             retries: Int = Const.clusterRetries,
             cacheError: Boolean = false
             ): Try = {

    assert(trace.size == 1)

    Try(trace.head)(retries, cacheError)
  }
}

final case class Try(
                      override val self: Seq[Action])(
                      retries: Int,
                      override val cacheEmptyOutput: Boolean
                      ) extends Block(self) {

  override def trunk = Some(Try(this.trunkSeq)(retries, cacheEmptyOutput).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Page] = {

    val taskContext = TaskContext.get()

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
        val logger = LoggerFactory.getLogger(this.getClass)
        val timesLeft = retries - taskContext.attemptNumber()
        if (timesLeft > 0) {
          throw new TryException(
            s"Retrying cluster-wise on ${e.getClass.getSimpleName}... $timesLeft time(s) left\n" +
              "(if Spark job failed because of this, please increase your spark.task.maxFailures)" +
              this.getActionExceptionMessage(session),
            e
          )
        }
        else logger.warn(s"Failover on ${e.getClass.getSimpleName}: Cluster-wise retries has depleted... ")
        logger.info("\t\\-->", e)
    }

    pages
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] ={
    val seq = this.doInterpolateSeq(pageRow)
    if (seq.isEmpty) None
    else Some(this.copy(self = seq)(this.retries, this.cacheEmptyOutput).asInstanceOf[this.type])
  }
}

object Loop {

  def apply(
             trace: Set[Trace],
             limit: Int = Const.maxLoop
             ): Loop = {
    assert(trace.size == 1)

    Loop(trace.head, limit) //TODO: should persist rule of Cartesian join
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

//case class cannot be overridden
//syntax sugar for loop-click-wait
object LoadMore {

  import dsl._

  def apply(
             selector: String,
             limit: Int = Const.maxLoop,
             delay: Duration = Const.interactionDelayMin
             ): Loop =
    Loop(
      Click(selector, delay = delay),
      limit
    )
}

object Paginate {

  import dsl._

  def apply(
             selector: String,
             limit: Int = Const.maxLoop,
             delay: Duration = Const.interactionDelayMin
             ): Loop = {
    Loop(
      Snapshot()
        +> Click(selector, delay = delay),
      limit
    )
  }
}

final case class If(
                     condition: Page => Boolean,
                     ifTrue: Seq[Action] = Nil,
                     ifFalse: Seq[Action] = Nil
                     ) extends Block(ifTrue ++ ifFalse) {

  override def trunk = Some(this.copy(ifTrue = ifTrue.flatMap(_.trunk), ifFalse = ifFalse.flatMap(_.trunk)).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Page] = {

    val current = DefaultSnapshot.exe(session).head.asInstanceOf[Page]

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