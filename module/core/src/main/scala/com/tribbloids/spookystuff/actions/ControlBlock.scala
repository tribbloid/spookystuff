package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.*
import com.tribbloids.spookystuff.actions.HasTrace
import com.tribbloids.spookystuff.actions.Wayback
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.CommonUtils
import com.tribbloids.spookystuff.doc.Observation
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Only for complex workflow control, each defines a nested/non-linear subroutine that may or may not be executed once
  * or multiple times depending on situations.
  */
abstract class ControlBlock(
    override val trace: Trace
) extends Actions
    with MayExport
    with Wayback {

  def _copy(children: Trace): ControlBlock

  override def wayback: Option[Long] =
    trace
      .flatMap {
        case w: Wayback => Some(w)
        case _          => None
      }
      .lastOption
      .flatMap {
        _.wayback
      }

  final override def doExe(agent: Agent): Seq[Observation] = {

    val doc = this.doExeNoUID(agent)

    val backtrace = (agent.backtrace :+ this).toList
    val result = doc.zipWithIndex.map { tuple =>
      {
        val fetched = tuple._1

        fetched.updated(
          uid = fetched.uid.copy(backtrace = backtrace, blockIndex = tuple._2, blockSize = doc.size)(nameOvrd =
            fetched.uid.nameOvrd
          )
        )
      }
    }
//    if (result.isEmpty && this.dryRun.hasOutput) {
//      Seq(NoDoc(backtrace, cacheLevel = this.cacheEmptyOutput))
//    } else if (result.count(_.isInstanceOf[Observation]) == 0 && this.hasOutput) {
//      result.map(_.updated(cacheLevel = this.cacheEmptyOutput))
//    } else {
//      result
//    }

    result
  }

  def doExeNoUID(agent: Agent): Seq[Observation]
}

object ControlBlock {

  val defaultLoop: Int = 16

  object ClusterRetry {
    // need a rewrite using multi-pass durable execution
  }

  object LocalRetry {

    def apply(
        trace: Trace,
        retries: Int = Const.clusterRetries
    ): LocalRetryImpl = {

      LocalRetryImpl(trace)(retries)
    }

    final case class LocalRetryImpl(
        override val trace: Trace
    )(
        retries: Int
    ) extends ControlBlock(trace) {

      @transient override lazy val stateChangeOnly: LocalRetryImpl = {

        this.copy(trace = trace.stateChangeOnly)(retries)
      }

      override def doExeNoUID(agent: Agent): Seq[Observation] = {

        val pages = new ArrayBuffer[Observation]()

        try {
          for (action <- trace) {
            pages ++= action.exe(agent)
          }
        } catch {
          case _: Exception =>
            CommonUtils.retry(retries) {
              val retriedPages = new ArrayBuffer[Observation]()

              for (action <- trace) {
                retriedPages ++= action.exe(agent)
              }
              retriedPages
            }
        }

        pages.toSeq
      }

      override def _copy(children: Trace): ControlBlock = this.copy(children)(retries)
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
      override val trace: Trace,
      limit: Int = ControlBlock.defaultLoop
  ) extends ControlBlock(trace) {

    assert(limit > 0)

    override def doExeNoUID(agent: Agent): Seq[Observation] = {

      val pages = new ArrayBuffer[Observation]()

      try {
        for (_ <- 0 until limit) {
          for (action <- trace.trace) {
            pages ++= action.exe(agent)
          }
        }
      } catch {
        case e: Exception =>
          LoggerFactory.getLogger(this.getClass).info("Aborted on exception: " + e)
      }

      pages.toSeq
    }

    override def _copy(children: Trace): ControlBlock = this.copy(children)

    override def stateChangeOnly: HasTrace = {
      this.copy(trace.stateChangeOnly)
    }
  }

}
// TODO: should be removed completely after define-by-run & tracing API is implemented
