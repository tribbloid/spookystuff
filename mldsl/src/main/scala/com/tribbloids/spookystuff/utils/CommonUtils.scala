package com.tribbloids.spookystuff.utils

import java.io.{File, InputStream}
import java.net.URL

import org.apache.spark.SparkEnv
import org.apache.spark.ml.dsl.utils.FlowUtils
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

  // TODO: remove, use object API everywhere.
  def retry: RetryFixedInterval.type = RetryFixedInterval

  protected def _callerShowStr: String = {
    val result = FlowUtils
      .Caller(
        exclude = Seq(classOf[CommonUtils])
      )
      .showStr
    result
  }

//  def isolatedExecutionContext = {
//
//    // TODO: switch to cached thread pool with inifite size
//    val threadPool = Executors.newSingleThreadExecutor()
//    val ctx = ExecutionContext.fromExecutor(threadPool)
//    ctx
//  }

  def withDeadline[T](
      n: Duration,
      heartbeatOpt: Option[Duration] = Some(10.seconds)
  )(
      fn: => T,
      callbackOpt: Option[Int => Unit] = None
  ): T = {

    val future = FutureInterruptable(fn)(AwaitWithHeartbeat.executionContext)

    val TIMEOUT = "TIMEOUT!!!!" + s"\t@ ${_callerShowStr}"

    try {
      val hb = AwaitWithHeartbeat(heartbeatOpt)(callbackOpt)
      hb.result(future, n)
    } catch {
      case e: TimeoutException =>
        future.interrupt()
        LoggerFactory.getLogger(this.getClass).debug(TIMEOUT)
        throw e
    }
  }

  def getCPResource(str: String): Option[URL] =
    Option(ClassLoader.getSystemClassLoader.getResource(str.stripSuffix(File.separator)))
  def getCPResourceAsStream(str: String): Option[InputStream] =
    Option(ClassLoader.getSystemClassLoader.getResourceAsStream(str.stripSuffix(File.separator)))

  def getCPResourceDebugInfo(str: String): String = {
    val urlOpt = getCPResource(str)
    val info = urlOpt match {
      case Some(url) =>
        s"\tresource `$str` refers to ${url.toString}"
      case None =>
        s"\tresource `$str` has no reference"
    }
    info
  }

  def debugCPResource(
      classpathFiles: Seq[String] = List(
        "log4j.properties",
        "rootkey.csv",
        ".rootkey.csv"
      )
  ): Unit = {

    {
      val resolvedInfos = classpathFiles.map { v =>
        CommonUtils.getCPResourceDebugInfo(v)
      }
      println("resolving files in classpath ...\n" + resolvedInfos.mkString("\n"))
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

  private val useTaskLocationStrV2: Boolean =
    try { // some classloaders won't load org.apache.spark.package$ by default, hence the bypass
      org.apache.spark.SPARK_VERSION.substring(0, 3).toDouble >= 1.6
    } catch {
      case e: Throwable =>
        true
    }

  /**
    * From doc of org.apache.spark.scheduler.TaskLocation
    * Create a TaskLocation from a string returned by getPreferredLocations.
    * These strings have the form executor_[hostname]_[executorid], [hostname], or
    * hdfs_cache_[hostname], depending on whether the location is cached.
    * def apply(str: String): TaskLocation
    * ...
    * Not sure if it will change in future Spark releases
    */
  def taskLocationStrOpt: Option[String] = {

    blockManagerIDOpt.map { bmID =>
      val hostPort = bmID.hostPort

      if (useTaskLocationStrV2) {
        val executorID = bmID.executorId
        s"executor_${hostPort}_$executorID"
      } else {
        hostPort
      }
    }
  }

  def toStrNullSafe(v: Any): String = "" + v

  def tryParseBoolean(str: => String): Try[Boolean] = {
    Try { str }.flatMap { v =>
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
}

object CommonUtils extends CommonUtils
