package com.tribbloids.spookystuff.utils

import java.io.{File, InputStream}
import java.net.URL

import org.apache.spark.ml.dsl.utils.FlowUtils
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future, TimeoutException}
import scala.reflect.ClassTag
import scala.util.Random

abstract class CommonUtils {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  lazy val scalaVersion: String = scala.util.Properties.versionNumberString
  lazy val scalaBinaryVersion = scalaVersion.split('.').slice(0, 2).mkString(".")

  def numDriverCores = {
    val result = Runtime.getRuntime.availableProcessors()
    assert(result > 0)
    result
  }

  def qualifiedName(separator: String)(parts: String*) = {
    parts.flatMap(v => Option(v)).reduceLeftOption(addSuffix(separator, _) + _).orNull
  }
  def addSuffix(suffix: String, part: String) = {
    if (part.endsWith(suffix)) part
    else part+suffix
  }

  def /:/(parts: String*): String = qualifiedName("/")(parts: _*)
  def :/(part: String): String = addSuffix("/", part)

  def \\\(parts: String*): String = {
    val _parts = parts.flatMap {
      v =>
        Option(v).map (
          _.replace('/',File.separatorChar)
        )
    }
    qualifiedName(File.separator)(_parts: _*)
  }
  def :\(part: String): String = {
    val _part = part.replace('/',File.separatorChar)
    addSuffix(File.separator, _part)
  }

  // TODO: remove, use object API everywhere.
  def retry = RetryFixedInterval


  protected def _callerShowStr = {
    val result = FlowUtils.callerShowStr(
      exclude = Seq(classOf[CommonUtils])
    )
    result
  }

  def withDeadline[T](
                       n: Duration,
                       heartbeatOpt: Option[Duration] = Some(10.seconds)
                     )(
                       fn: =>T,
                       callbackOpt: Option[Int => Unit] = None
                     ): T = {

    @transient var thread: Thread = null
    val future = Future {
      thread = Thread.currentThread()
      fn
    }

    lazy val TIMEOUT = "TIMEOUT!!!!" + s"\t@ ${_callerShowStr}"

    try {
      val hb = AwaitWithHeartbeat(heartbeatOpt)(callbackOpt)
      hb.result(future, n)
    }
    catch {
      case e: TimeoutException =>
        Option(thread).foreach(_.interrupt())
        LoggerFactory.getLogger(this.getClass).debug(TIMEOUT)
        throw e
    }
  }

  case class AwaitWithHeartbeat(
                                 intervalOpt: Option[Duration] = Some(10.seconds)
                               )(callbackOpt: Option[Int => Unit] = None) {

    def result[T](future: Future[T], n: Duration): T = {
      val nMillis = n.toMillis

      intervalOpt match {
        case None =>
          Await.result(future, n)
        case Some(heartbeat) =>
          val startTime = System.currentTimeMillis()
          val terminateAt = startTime + nMillis

          val effectiveHeartbeatFn: Int => Unit = callbackOpt.getOrElse {
            i =>
              val remainMillis = terminateAt - System.currentTimeMillis()
              LoggerFactory.getLogger(this.getClass).info(
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
            }
            catch {
              case e: TimeoutException if heartbeatMillis < remainMillis =>
            }
          }
          throw new UnknownError("IMPOSSIBLE")
      }
    }
  }

  def getCPResource(str: String): Option[URL] =
    Option(ClassLoader.getSystemClassLoader.getResource(str.stripSuffix(File.separator)))
  def getCPResourceAsStream(str: String): Option[InputStream] =
    Option(ClassLoader.getSystemClassLoader.getResourceAsStream(str.stripSuffix(File.separator)))

  @scala.annotation.tailrec
  final def unboxException[T <: Throwable: ClassTag](e: Throwable): Throwable = {
    e match {
      case ee: T =>
        unboxException[T](ee.getCause)
      case _ =>
        e
    }
  }

  def timer[T](fn: => T): (T, Long) = {
    val startTime = System.currentTimeMillis()
    val result = fn
    val endTime = System.currentTimeMillis()
    (result, endTime - startTime)
  }

  def randomSuffix = Math.abs(Random.nextLong())

  def randomChars: String = {
    val len = Random.nextInt(128)
    Random.nextString(len)
  }

}

object CommonUtils extends CommonUtils
