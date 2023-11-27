package com.tribbloids.spookystuff.utils

import ai.acyclic.prover.commons.debug.Debug.CallStackRef
import com.tribbloids.spookystuff.utils.AwaitWithHeartbeat.Heartbeat

import java.io.{File, PrintWriter, StringWriter}
import org.apache.spark.SparkEnv
import org.apache.spark.storage.BlockManagerId
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.concurrent.TimeoutException
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}

abstract class CommonUtils {

  import scala.concurrent.duration._

  lazy val scalaVersion: String = scala.util.Properties.versionNumberString
  lazy val scalaBinaryVersion: String = scalaVersion.split('.').slice(0, 2).mkString(".")

  def numLocalCores: Int = {
    val result = Runtime.getRuntime.availableProcessors()
    assert(result > 0)
    result
  }

  def qualifiedName(separator: String)(parts: String*): String = {
    parts.flatMap(v => Option(v)).reduceLeftOption(addSuffix(separator, _) + _).orNull
  }
  def addSuffix(suffix: String, part: String): String = {
    if (part.endsWith(suffix)) part
    else part + suffix
  }

  def /:/(parts: String*): String = qualifiedName("/")(parts: _*)
  def :/(part: String): String = addSuffix("/", part)

  def \\\(parts: String*): String = {
    val _parts = parts.flatMap { v =>
      Option(v).map(
        _.replace('/', File.separatorChar)
      )
    }
    qualifiedName(File.separator)(_parts: _*)
  }
  def :\(part: String): String = {
    val _part = part.replace('/', File.separatorChar)
    addSuffix(File.separator, _part)
  }

  def uri2fileName(path: String): String = path.split(File.separatorChar).last

  // TODO: remove, use object API everywhere.
  def retry: Retry.FixedInterval.type = Retry.FixedInterval

  protected def _callerShowStr: String = {
    val result = CallStackRef
      .below(
        condition = _.isUnderClasses(classOf[CommonUtils])
      )
      .showStr
    result
  }

  def withTimeout[T](
      timeout: Timeout,
      interval: Duration = 10.seconds,
      interrupt: Boolean = true
  )(
      fn: => T,
      heartbeat: Heartbeat = Heartbeat.default
  ): T = {

    val future = FutureInterruptable(fn)(AwaitWithHeartbeat.executionContext)

    val TIMEOUT = "TIMEOUT!!!!" + s"\t@ ${_callerShowStr}"

    try {
      val await = AwaitWithHeartbeat(interval)(heartbeat)
      await.result(future, timeout)
    } catch {
      case e: TimeoutException =>
        if (interrupt) future.interrupt()
        LoggerFactory.getLogger(this.getClass).debug(TIMEOUT)
        throw e
    }
  }

  @scala.annotation.tailrec
  final def unboxException[T <: Throwable: ClassTag](e: Throwable): Throwable = {
    e match {
      case ee: T =>
        unboxException[T](ee.getCause)
      case _ =>
        e
    }
  }

  def timed[T](fn: => T): (T, Long) = {

    Stopwatch() {
      fn
    }.exportAs(v => v.split)
  }

  def randomSuffix: Long = Math.abs(Random.nextLong())

  def randomChars: String = {
    val len = Random.nextInt(128)
    Random.nextString(len)
  }

  def blockManagerIDOpt: Option[BlockManagerId] = {
    Option(SparkEnv.get).map(v => v.blockManager.blockManagerId)
  }

  def toStrNullSafe(v: Any): String = "" + v

  def tryParseBoolean(str: => String): Try[Boolean] = {
    Try(str).flatMap { v =>
      v.toLowerCase match {
        case "true" | "1" | ""    => Success(true)
        case "false" | "0" | "-1" => Success(false)
        case _ =>
          Failure(
            new UnsupportedOperationException(
              s"$v is not a boolean value"
            )
          )
      }
    }
  }

  def orderedGroupBy[T, R](vs: Seq[T])(fn: T => R): Seq[(R, Seq[T])] = {

    val keys = vs.map(fn).distinct
    val grouped = vs.groupBy(fn)
    val orderedGrouped = keys.map(key => key -> grouped(key))
    orderedGrouped
  }

  def mergePreserveOrder[K, V](
      x: Iterable[(K, V)],
      y: Iterable[(K, V)]
  ): ListMap[K, V] = {

    val proto = orderedGroupBy((x ++ y).toSeq)(_._1)
    val proto2 = proto.map {
      case (k, v) => k -> v.head._2
    }
    ListMap(proto2: _*)
  }

  // copied from org.apache.spark.util.Utils
  def stacktraceStr(e: Throwable): String = {
    if (e == null) {
      ""
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }
}

object CommonUtils extends CommonUtils
