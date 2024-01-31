package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.actions.Wayback.WaybackLike
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc.{NoDoc, Observation}
import com.tribbloids.spookystuff.extractors.Extractor
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.utils.http.HttpUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Only for complex workflow control, each defines a nested/non-linear subroutine that may or may not be executed once
  * or multiple times depending on situations.
  */
abstract class Block(
    override val children: Trace
) extends Actions
    with Named
    with WaybackLike {

  override def wayback: Extractor[Long] =
    children
      .flatMap {
        case w: WaybackLike => Some(w)
        case _              => None
      }
      .lastOption
      .map {
        _.wayback
      }
      .orNull

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

  def cacheEmptyOutput: DocCacheLevel.Value = DocCacheLevel.All

  final override def doExe(agent: Agent): Seq[Observation] = {

    val doc = this.doExeNoUID(agent)

    val backtrace = (agent.backtrace :+ this).toList
    val result = doc.zipWithIndex.map { tuple =>
      {
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
    } else if (result.count(_.isInstanceOf[Observation]) == 0 && this.hasOutput) {
      result.map(_.updated(cacheLevel = this.cacheEmptyOutput))
    } else {
      result
    }
  }

  def doExeNoUID(agent: Agent): Seq[Observation]
}

object ClusterRetry {

  def apply(
      trace: HasTrace,
      retries: Int = Const.clusterRetries,
      cacheEmptyOutput: DocCacheLevel.Value = DocCacheLevel.NoCache
  ): ClusterRetryImpl = {

    ClusterRetryImpl(trace)(retries, cacheEmptyOutput)
  }

  // TODO: this retry mechanism use Spark scheduler to re-run the partition and is very inefficient
  //  Re-implement using multi-pass!
  final case class ClusterRetryImpl private (
      override val children: Trace
  )(
      retries: Int,
      override val cacheEmptyOutput: DocCacheLevel.Value
  ) extends Block(children) {

    override def skeleton: Option[ClusterRetryImpl.this.type] =
      Some(ClusterRetryImpl(this.childrenSkeleton)(retries, cacheEmptyOutput).asInstanceOf[this.type])

    override def doExeNoUID(agent: Agent): Seq[Observation] = {

      val pages = new ArrayBuffer[Observation]()

      try {
        for (action <- children) {
          pages ++= action.exe(agent)
        }
      } catch {
        case e: Exception =>
          val logger = LoggerFactory.getLogger(this.getClass)
          // avoid endless retry if tcOpt is missing
          val timesLeft = retries - agent.taskContextOpt.map(_.attemptNumber()).getOrElse(Int.MaxValue)
          if (timesLeft > 0) {
            throw new RetryingException(
              s"Retrying cluster-wise on ${e.getClass.getSimpleName}... $timesLeft time(s) left\n" +
                "(if Spark job failed because of this, please increase your spark.task.maxFailures)" +
                this.getSessionExceptionMessage(agent),
              e
            )
          } else logger.warn(s"Failover on ${e.getClass.getSimpleName}: Cluster-wise retries has depleted")
          logger.debug("\t\\-->", e)
      }

      pages.toSeq
    }

    override def doInterpolate(row: FetchedRow, schema: SpookySchema): Option[this.type] = {
      val opt = this.doInterpolateSeq(row, schema)
      opt.map { seq =>
        this.copy(children = seq)(this.retries, this.cacheEmptyOutput).asInstanceOf[this.type]
      }

    }
  }
}

object LocalRetry {

  def apply(
      trace: Trace,
      retries: Int = Const.clusterRetries,
      cacheEmptyOutput: DocCacheLevel.Value
  ): LocalRetryImpl = {

    LocalRetryImpl(trace)(retries, cacheEmptyOutput)
  }

  final case class LocalRetryImpl(
      override val children: Trace
  )(
      retries: Int,
      override val cacheEmptyOutput: DocCacheLevel.Value
  ) extends Block(children) {

    override def skeleton: Option[LocalRetryImpl.this.type] =
      Some(LocalRetryImpl(this.childrenSkeleton)(retries, cacheEmptyOutput).asInstanceOf[this.type])

    override def doExeNoUID(agent: Agent): Seq[Observation] = {

      val pages = new ArrayBuffer[Observation]()

      try {
        for (action <- children) {
          pages ++= action.exe(agent)
        }
      } catch {
        case _: Exception =>
          CommonUtils.retry(retries) {
            val retriedPages = new ArrayBuffer[Observation]()

            for (action <- children) {
              retriedPages ++= action.exe(agent)
            }
            retriedPages
          }
      }

      pages.toSeq
    }

    override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema): Option[this.type] = {
      val opt = this.doInterpolateSeq(pageRow, schema)
      opt.map { seq =>
        this.copy(children = seq)(this.retries, this.cacheEmptyOutput).asInstanceOf[this.type]
      }
    }
  }
}

object Loop {}

/**
  * Contains several sub-actions that are iterated for multiple times Will iterate until max iteration is reached or
  * execution is impossible (sub-action throws an exception)
  *
  * @param limit
  *   max iteration, default to Const.fetchLimit
  * @param arg
  *   a list of actions being iterated through
  */
final case class Loop(
    override val children: Trace,
    limit: Int = Const.maxLoop
) extends Block(children) {

  assert(limit > 0)

  override def skeleton: Option[Loop.this.type] =
    Some(this.copy(children = this.childrenSkeleton).asInstanceOf[this.type])

  override def doExeNoUID(agent: Agent): Seq[Observation] = {

    val pages = new ArrayBuffer[Observation]()

    try {
      for (_ <- 0 until limit) {
        for (action <- children.trace) {
          pages ++= action.exe(agent)
        }
      }
    } catch {
      case e: Exception =>
        LoggerFactory.getLogger(this.getClass).info("Aborted on exception: " + e)
    }

    pages.toSeq
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema): Option[this.type] = {
    val opt = this.doInterpolateSeq(pageRow, schema)
    opt.map { seq =>
      this.copy(children = seq).asInstanceOf[this.type]
    }
  }
}

@SerialVersionUID(8623719358582480968L)
case class OAuthV2(self: Wget) extends Block(self) with Driverless {

  def rewrite(agent: Agent): Wget = {

    val keys = agent.spooky.spookyConf.oAuthKeysFactory.apply()
    if (keys == null) {
      throw new QueryException("need to set SpookyConf.oAuthKeys first")
    }
    val effectiveWget: Wget = self.uriOption match {
      case Some(uri) =>
        val signed =
          HttpUtils.OauthV2(uri.toString, keys.consumerKey, keys.consumerSecret, keys.token, keys.tokenSecret)
        self.copy(uri = Lit.erased(signed))
      case None =>
        self
    }
    effectiveWget
  }

  override def skeleton: Option[OAuthV2.this.type] = Some(this)

  override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema): Option[this.type] =
    self.interpolate(pageRow, schema).map { v =>
      this.copy(self = v).asInstanceOf[this.type]
    }

  override def doExeNoUID(agent: Agent): Seq[Observation] = {
    val effectiveWget = this.rewrite(agent)

    effectiveWget
      .exe(agent)
  }
}

final case class AndThen(
    self: Action,
    f: Seq[Observation] => Seq[Observation]
) extends Block(self) {

  override def skeleton: Option[AndThen.this.type] = Some(this)

  override def doExeNoUID(agent: Agent): Seq[Observation] = {
    f(self.exe(agent))
  }
}
