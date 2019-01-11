package com.tribbloids.spookystuff.utils

import java.util.concurrent.Executors

import org.apache.spark.ml.dsl.utils.FlowUtils
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}

object AwaitWithHeartbeat {

  val threadPool = Executors.newCachedThreadPool()
  val executionContext = ExecutionContext.fromExecutor(threadPool)
}

case class AwaitWithHeartbeat(
    intervalOpt: Option[Duration] = Some(10.seconds)
)(callbackOpt: Option[Int => Unit] = None) {

  protected lazy val _callerShowStr = {
    val result = FlowUtils.callerShowStr(
      exclude = Seq(classOf[CommonUtils], classOf[AwaitWithHeartbeat])
    )
    result
  }

  def result[T](future: Future[T], n: Duration): T = {
    val nMillis = n.toMillis

    intervalOpt match {
      case None =>
        Await.result(future, n)
      case Some(heartbeat) =>
        val startTime = System.currentTimeMillis()
        val terminateAt = startTime + nMillis

        val effectiveHeartbeatFn: Int => Unit = callbackOpt.getOrElse { i: Int =>
          val remainMillis = terminateAt - System.currentTimeMillis()
          if (i > 1)
            LoggerFactory
              .getLogger(this.getClass)
              .info(
                s"T - ${remainMillis.toDouble / 1000} second(s)" +
                  "\t@ " + _callerShowStr
              )
        }

        val heartbeatMillis = heartbeat.toMillis
        for (i <- 0 to (nMillis / heartbeatMillis).toInt) {
          val remainMillis = Math.max(terminateAt - System.currentTimeMillis(), 0L)
          effectiveHeartbeatFn(i)
          val epochMillis = Math.min(heartbeatMillis, remainMillis)
          try {
            val result = Await.result(future, epochMillis.milliseconds)
            return result
          } catch {
            case e: TimeoutException if heartbeatMillis < remainMillis =>
          }
        }
        throw new UnknownError("IMPOSSIBLE")
    }
  }
}
