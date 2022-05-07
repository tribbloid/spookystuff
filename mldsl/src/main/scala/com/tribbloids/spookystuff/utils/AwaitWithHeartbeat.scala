package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.AwaitWithHeartbeat.Heartbeat
import org.apache.spark.ml.dsl.utils.DSLUtils
import org.slf4j.LoggerFactory

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.duration._
import scala.concurrent._

object AwaitWithHeartbeat {

  val threadPool: ExecutorService = Executors.newCachedThreadPool()
  val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(threadPool)

  trait Heartbeat {

    def tryEmit(
        i: Int,
        remainMil: Long,
        callerStr: String
    ): Boolean
  }

  object Heartbeat {

    object Always extends Heartbeat {
      override def tryEmit(i: Int, remainMil: Long, callerStr: String): Boolean = true
    }

    case class WrapWithInfo(delegate: Heartbeat) extends Heartbeat {

      override def tryEmit(i: Int, remainMil: Long, callerStr: String): Boolean = {

        val result = delegate.tryEmit(i, remainMil, callerStr)

        if (i > 1) {
          val infoStr = s"T - ${remainMil.toDouble / 1000} second(s)" +
            "\t@ " + callerStr

          val logger = LoggerFactory.getLogger(this.getClass)
          if (result) {
            logger.info(infoStr)
          } else {
            logger.warn(s"No Heartbeat, $infoStr")
          }
        }

        result
      }
    }

    val default: WrapWithInfo = WrapWithInfo(Always)

    implicit class fromFn1(fn: Int => Boolean) extends Heartbeat {
      override def tryEmit(i: Int, remainMil: Long, callerStr: String): Boolean = fn(i)
    }
  }
}

case class AwaitWithHeartbeat(
    interval: Duration = 10.seconds
)(heartbeat: Heartbeat = Heartbeat.default) {

  protected lazy val _callerShowStr: String = {
    val result = DSLUtils
      .Caller(
        exclude = Seq(classOf[CommonUtils], classOf[AwaitWithHeartbeat])
      )
      .showStr
    result
  }

  def result[T](future: Future[T], timeout: TimeoutConf): T = {
    val maxTimeMil = timeout.max.toMillis
    val maxNoProgressMil = timeout.noProgress.toMillis

    interval match {
      case Duration.Inf =>
        Await.result(future, timeout.max)
      case _ =>
        val intervalMil = interval.toMillis
        val startTime = System.currentTimeMillis()
        val maxDeadline = startTime + maxTimeMil

        var noProgressDeadline = System.currentTimeMillis() + maxNoProgressMil

        for (i <- 0 to (maxTimeMil / intervalMil).toInt) {

          val current = System.currentTimeMillis()
          val remain = Math.max(Math.min(maxDeadline, noProgressDeadline) - current, 0)
          val isHeartbeat = heartbeat.tryEmit(i, remain, _callerShowStr)
          if (isHeartbeat) {
            noProgressDeadline = maxNoProgressMil + current
          }

          val nextEpochMil = Math.min(intervalMil, remain)
          try {
            val result = Await.result(future, nextEpochMil.milliseconds)
            return result
          } catch {
            case _: TimeoutException if intervalMil < remain =>
          }
        }
        throw new UnknownError("IMPOSSIBLE")
    }
  }
}
