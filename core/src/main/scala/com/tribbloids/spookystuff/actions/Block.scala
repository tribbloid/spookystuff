package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.caching.CacheLevel
import com.tribbloids.spookystuff.doc.{Doc, Fetched, NoDoc}
import com.tribbloids.spookystuff.extractors.Extractor
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.http.HttpUtils
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.SpookyUtils.retry
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
  * Only for complex workflow control,
  * each defines a nested/non-linear subroutine that may or may not be executed
  * once or multiple times depending on situations.
  */
abstract class Block(override val children: Trace) extends Actions(children) with Named with Wayback {

  //  assert(self.nonEmpty)

  override def wayback: Extractor[Long] = children.flatMap {
    case w: Wayback => Some(w)
    case _ => None
  }.lastOption.map {
    _.wayback
  }.orNull

  //  override def as(name: Symbol) = {
  //    super.as(name)
  //
  //    children.foreach{
  //      case n: Named => n.as(name)
  //      case _ =>
  //    }
  //
  //    this
  //  }

  def cacheEmptyOutput: CacheLevel.Value = CacheLevel.All

  final override def doExe(session: Session): Seq[Fetched] = {

    val doc = this.doExeNoUID(session)

    val backtrace = (session.backtrace :+ this).toList
    val result = doc.zipWithIndex.map {
      tuple => {
        val fetched = tuple._1

        val updatedName = this.nameOpt.getOrElse {
          fetched.uid.name
        }
        fetched.updated(
          uid = fetched.uid.copy(backtrace = backtrace, blockIndex = tuple._2, blockSize = doc.size)(name = updatedName)
        )
      }
    }
    if (result.isEmpty && this.hasOutput) {
      Seq(NoDoc(backtrace, cacheLevel = this.cacheEmptyOutput))
    }
    else if (result.count(_.isInstanceOf[Fetched]) == 0 && this.hasOutput) {
      result.map(_.updated(cacheLevel = this.cacheEmptyOutput))
    }
    else {
      result
    }
  }

  def doExeNoUID(session: Session): Seq[Fetched]
}

object ClusterRetry {

  def apply(
             trace: Set[Trace],
             retries: Int = Const.clusterRetries,
             cacheEmptyOutput: CacheLevel.Value = CacheLevel.NoCache
           ): ClusterRetry = {

    assert(trace.size <= 1)

    ClusterRetry(trace.headOption.getOrElse(Actions.empty))(retries, cacheEmptyOutput)
  }
}

final case class ClusterRetry(
                               override val children: Trace
                             )(
                               retries: Int,
                               override val cacheEmptyOutput: CacheLevel.Value
                             ) extends Block(children) {

  override def skeleton = Some(ClusterRetry(this.trunkSeq)(retries, cacheEmptyOutput).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Fetched] = {

    val pages = new ArrayBuffer[Fetched]()

    try {
      for (action <- children) {
        pages ++= action.exe(session)
      }
    }
    catch {
      case e: Throwable =>
        val logger = LoggerFactory.getLogger(this.getClass)
        //avoid endless retry if tcOpt is missing
        val timesLeft = retries - session.taskContextOpt.map(_.attemptNumber()).getOrElse(Int.MaxValue)
        if (timesLeft > 0) {
          throw new RetryingException(
            s"Retrying cluster-wise on ${e.getClass.getSimpleName}... $timesLeft time(s) left\n" +
              "(if Spark job failed because of this, please increase your spark.task.maxFailures)" +
              this.getSessionExceptionString(session),
            e
          )
        }
        else logger.warn(s"Failover on ${e.getClass.getSimpleName}: Cluster-wise retries has depleted")
        logger.info("\t\\-->", e)
    }

    pages
  }

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] ={
    val seq = this.doInterpolateSeq(pageRow, schema)
    if (seq.isEmpty) None
    else Some(this.copy(children = seq)(this.retries, this.cacheEmptyOutput).asInstanceOf[this.type])
  }
}

object LocalRetry {

  def apply(
             trace: Set[Trace],
             retries: Int = Const.clusterRetries,
             cacheEmptyOutput: CacheLevel.Value
           ): LocalRetry = {

    assert(trace.size <= 1)

    LocalRetry(trace.headOption.getOrElse(Actions.empty))(retries, cacheEmptyOutput)
  }
}

final case class LocalRetry(
                             override val children: Trace
                           )(
                             retries: Int,
                             override val cacheEmptyOutput: CacheLevel.Value
                           ) extends Block(children) {

  override def skeleton = Some(LocalRetry(this.trunkSeq)(retries, cacheEmptyOutput).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Fetched] = {

    val pages = new ArrayBuffer[Fetched]()

    try {
      for (action <- children) {
        pages ++= action.exe(session)
      }
    }
    catch {
      case e: Throwable =>
        retry[Seq[Fetched]](retries)({
          val retriedPages = new ArrayBuffer[Fetched]()

          for (action <- children) {
            retriedPages ++= action.exe(session)
          }
          retriedPages
        })
    }

    pages
  }

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] ={
    val seq = this.doInterpolateSeq(pageRow, schema)
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

    Loop(trace.head, limit) //TODO: should persist rule of Cartesian join & yield Set[Loop]
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

  override def skeleton = Some(this.copy(children = this.trunkSeq).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Fetched] = {

    val pages = new ArrayBuffer[Fetched]()

    try {
      for (i <- 0 until limit) {
        for (action <- children) {
          pages ++= action.exe(session)
        }
      }
    }
    catch {
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).info("Aborted on exception: " + e)
    }

    pages
  }

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] ={
    val seq = this.doInterpolateSeq(pageRow, schema)
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
             delay: Duration = Const.Interaction.delayMin
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
             delay: Duration = Const.Interaction.delayMin
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
             condition: DocCondition,
             ifTrue: Set[Trace] = Set(),
             ifFalse: Set[Trace] = Set()
           ): If = {

    assert(ifTrue.size <= 1)
    assert(ifFalse.size <= 1)

    If(
      condition,
      ifTrue.headOption.getOrElse(Actions.empty),
      ifFalse.headOption.getOrElse(Actions.empty)
    ) //TODO: should persist rule of Cartesian join & yield Set[Loop]
  }
}

final case class If(
                     condition: DocCondition, //TODO: merge with Extraction[Boolean]
                     ifTrue: Trace,
                     ifFalse: Trace
                   ) extends Block(ifTrue ++ ifFalse) {

  override def skeleton = Some(this.copy(ifTrue = ifTrue.flatMap(_.skeleton), ifFalse = ifFalse.flatMap(_.skeleton)).asInstanceOf[this.type])

  override def doExeNoUID(session: Session): Seq[Fetched] = {

    val current = QuickSnapshot.exe(session).head.asInstanceOf[Doc]

    val pages = new ArrayBuffer[Fetched]()
    if (condition(current, session)) {
      for (action <- ifTrue) {
        pages ++= action.exe(session)
      }
    }
    else {
      for (action <- ifFalse) {
        pages ++= action.exe(session)
      }
    }

    pages
  }

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] ={
    val ifTrueInterpolated = Actions.doInterppolateSeq(ifTrue, pageRow, schema)
    val ifFalseInterpolated = Actions.doInterppolateSeq(ifFalse, pageRow, schema)
    val result = this.copy(ifTrue = ifTrueInterpolated, ifFalse = ifFalseInterpolated).asInstanceOf[this.type]
    Some(result)
  }
}

@SerialVersionUID(8623719358582480968L)
case class OAuthV2(self: Wget) extends Block(List(self)) with Driverless {

  def rewrite(session: Session): Wget = {

    val keys = session.spooky.spookyConf.oAuthKeysFactory.apply()
    if (keys == null) {
      throw new QueryException("need to set SpookyConf.oAuthKeys first")
    }
    val effectiveWget: Wget = self.uriOption match {
      case Some(uri) =>
        val signed = HttpUtils.OauthV2(uri.toString, keys.consumerKey, keys.consumerSecret, keys.token, keys.tokenSecret)
        self.copy(uri = Lit.erased(signed))
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

  override def skeleton = Some(this)

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] =
    self.interpolate(pageRow, schema).map {
      v => this.copy(self = v.asInstanceOf[Wget]).asInstanceOf[this.type]
    }

  override def doExeNoUID(session: Session): Seq[Fetched] = {
    val effectiveWget = this.rewrite(session)

    effectiveWget
      .exe(session)
  }
}

final case class AndThen(self: Action, f: Seq[Fetched] => Seq[Fetched]) extends Block(List(self)) {

  override def skeleton = Some(this)

  override def doExeNoUID(session: Session): Seq[Fetched] = {
    f(self.exe(session))
  }
}