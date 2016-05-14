package com.tribbloids.spookystuff.actions

import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory
import com.tribbloids.spookystuff.expressions.{Expression, Literal}
import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.http.HttpUtils
import com.tribbloids.spookystuff.row.FetchedRow
import com.tribbloids.spookystuff.doc.{Fetched, NoDoc, Doc}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.Utils.retry

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
  * Only for complex workflow control,
  * each defines a nested/non-linear subroutine that may or may not be executed
  * once or multiple times depending on situations.
  */
abstract class Block(override val children: Trace) extends Actions(children) with Named with Wayback {

  //  assert(self.nonEmpty)

  override def wayback: Expression[Long] = children.flatMap {
    case w: Wayback => Some(w)
    case _ => None
  }.lastOption.map {
    _.wayback
  }.orNull

  override def as(name: Symbol) = {
    super.as(name)

    children.foreach{
      case n: Named => n.as(name)
      case _ =>
    }

    this
  }

  def cacheEmptyOutput: Boolean = true

  override def needDriver = children.map(_.needDriver).reduce(_ || _)

  final override def doExe(session: Session): Seq[Fetched] = {

    val pages = this.doExeNoUID(session)

    val backtrace = (session.backtrace :+ this).toList
    val result = pages.zipWithIndex.map {
      tuple => {
        val page = tuple._1

        page.copy(uid = page.uid.copy(backtrace = backtrace, blockIndex = tuple._2, blockSize = pages.size))
      }
    }
    if (result.isEmpty && this.hasOutput) Seq(NoDoc(backtrace, cacheable = this.cacheEmptyOutput))
    else result
  }

  def doExeNoUID(session: Session): Seq[Doc]
}

object Try {

  def apply(
             trace: Set[Trace],
             retries: Int = Const.clusterRetries,
             cacheError: Boolean = false
           ): Try = {

    assert(trace.size <= 1)

    Try(trace.headOption.getOrElse(Nil))(retries, cacheError)
  }
}

final case class Try(
                      override val children: Trace)(
                      retries: Int,
                      override val cacheEmptyOutput: Boolean
                    ) extends Block(children) {

  override def trunk = Some(Try(this.trunkSeq)(retries, cacheEmptyOutput).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Doc] = {

    val taskContext = TaskContext.get()

    val pages = new ArrayBuffer[Doc]()

    try {
      for (action <- children) {
        pages ++= action.exe(session).flatMap{
          case page: Doc => Some(page)
          case noPage: NoDoc => None
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
              this.handleSessionException(session),
            e
          )
        }
        else logger.warn(s"Failover on ${e.getClass.getSimpleName}: Cluster-wise retries has depleted... ")
        logger.info("\t\\-->", e)
    }

    pages
  }

  override def doInterpolate(pageRow: FetchedRow, spooky: SpookyContext): Option[this.type] ={
    val seq = this.doInterpolateSeq(pageRow, spooky)
    if (seq.isEmpty) None
    else Some(this.copy(children = seq)(this.retries, this.cacheEmptyOutput).asInstanceOf[this.type])
  }
}

object TryLocally {

  def apply(
             trace: Set[Trace],
             retries: Int = Const.clusterRetries,
             cacheError: Boolean = false
           ): TryLocally = {

    assert(trace.size <= 1)

    TryLocally(trace.headOption.getOrElse(Nil))(retries, cacheError)
  }
}

final case class TryLocally(
                             override val children: Trace)(
                             retries: Int,
                             override val cacheEmptyOutput: Boolean
                           ) extends Block(children) {

  override def trunk = Some(TryLocally(this.trunkSeq)(retries, cacheEmptyOutput).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Doc] = {

    val pages = new ArrayBuffer[Doc]()

    try {
      for (action <- children) {
        pages ++= action.exe(session).flatMap{
          case page: Doc => Some(page)
          case noPage: NoDoc => None
        }
      }
    }
    catch {
      case e: Throwable =>
        retry[Seq[Doc]](retries)({
          val pages = new ArrayBuffer[Doc]()

          for (action <- children) {
            pages ++= action.exe(session).flatMap {
              case page: Doc => Some(page)
              case noPage: NoDoc => None
            }
          }
          pages
        })
    }

    pages
  }

  override def doInterpolate(pageRow: FetchedRow, spooky: SpookyContext): Option[this.type] ={
    val seq = this.doInterpolateSeq(pageRow, spooky)
    if (seq.isEmpty) None
    else Some(this.copy(children = seq)(this.retries, this.cacheEmptyOutput).asInstanceOf[this.type])
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
  *
  * @param limit max iteration, default to Const.fetchLimit
  * @param children a list of actions being iterated through
  */
final case class Loop(
                       override val children: Trace,
                       limit: Int
                     ) extends Block(children) {

  assert(limit>0)

  override def trunk = Some(this.copy(children = this.trunkSeq).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Doc] = {

    val pages = new ArrayBuffer[Doc]()

    try {
      for (i <- 0 until limit) {
        for (action <- children) {
          pages ++= action.exe(session).flatMap{
            case page: Doc => Some(page)
            case noPage: NoDoc => None
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

  override def doInterpolate(pageRow: FetchedRow, spooky: SpookyContext): Option[this.type] ={
    val seq = this.doInterpolateSeq(pageRow, spooky)
    if (seq.isEmpty) None
    else Some(this.copy(children = seq).asInstanceOf[this.type])
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

object If {

  def apply(
             condition: Doc => Boolean,
             ifTrue: Set[Trace] = Set(),
             ifFalse: Set[Trace] = Set()
           ): If = {


    assert(ifTrue.size <= 1)
    assert(ifFalse.size <= 1)

    If(
      condition,
      ifTrue.headOption.getOrElse(Nil),
      ifFalse.headOption.getOrElse(Nil)
    ) //TODO: should persist rule of Cartesian join
  }
}

final case class If(
                     condition: Doc => Boolean,
                     ifTrue: Trace,
                     ifFalse: Trace
                   ) extends Block(ifTrue ++ ifFalse) {

  override def trunk = Some(this.copy(ifTrue = ifTrue.flatMap(_.trunk), ifFalse = ifFalse.flatMap(_.trunk)).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Doc] = {

    val current = DefaultSnapshot.exe(session).head.asInstanceOf[Doc]

    val pages = new ArrayBuffer[Doc]()
    if (condition(current)) {
      for (action <- ifTrue) {
        pages ++= action.exe(session).flatMap{
          case page: Doc => Some(page)
          case noPage: NoDoc => None
        }
      }
    }
    else {
      for (action <- ifFalse) {
        pages ++= action.exe(session).flatMap{
          case page: Doc => Some(page)
          case noPage: NoDoc => None
        }
      }
    }

    pages
  }

  override def doInterpolate(pageRow: FetchedRow, spooky: SpookyContext): Option[this.type] ={
    val ifTrueInterpolated = Actions.doInterppolateSeq(ifTrue, pageRow, spooky)
    val ifFalseInterpolated = Actions.doInterppolateSeq(ifFalse, pageRow, spooky)
    val result = this.copy(ifTrue = ifTrueInterpolated, ifFalse = ifFalseInterpolated).asInstanceOf[this.type]
    Some(result)
  }
}

case class OAuthV2(self: Wget) extends Block(List(self)) with Driverless {

  def rewrite(session: Session): Wget = {

    val keys = session.spooky.conf.oAuthKeysFactory.apply()
    if (keys == null) {
      throw new QueryException("need to set SpookyConf.oAuthKeys first")
    }
    val effectiveWget: Wget = self.uriOption match {
      case Some(uri) =>
        val signed = HttpUtils.OauthV2(uri.toString, keys.consumerKey, keys.consumerSecret, keys.token, keys.tokenSecret)
        self.copy(uri = Literal(signed))
      case None =>
        self
    }
    effectiveWget
  }

  //  override def doExeNoName(session: Session): Seq[Fetched] = {
  //    val effectiveWget = this.rewrite(session)
  //
  //    effectiveWget.doExeNoName(session).map{
  //      case noPage: NoPage => noPage.copy(trace = List(this))
  //      case page: Page => page.copy(uid = PageUID(List(this),this))
  //    }
  //  }

  override def trunk = Some(this)

  override def doInterpolate(pageRow: FetchedRow, context: SpookyContext): Option[this.type] =
    self.interpolate(pageRow, context: SpookyContext).map {
      v => this.copy(self = v.asInstanceOf[Wget]).asInstanceOf[this.type]
    }

  override def doExeNoUID(session: Session): Seq[Doc] = {
    val effectiveWget = this.rewrite(session)

    effectiveWget
      .exe(session)
      .collect {
        case v: Doc => v
      }
  }
}

final case class AndThen(self: Action, f: Seq[Fetched] => Seq[Fetched]) extends Block(List(self)) {

  override def trunk = Some(this)

  override def doExeNoUID(session: Session): Seq[Doc] = {
    f(self.exe(session))
      .collect {
        case v: Doc => v
      }
  }
}