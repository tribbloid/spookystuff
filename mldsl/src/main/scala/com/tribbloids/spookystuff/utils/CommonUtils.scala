package com.tribbloids.spookystuff.utils

import java.io.{File, InputStream}
import java.net.URL

import org.apache.spark.SparkEnv
import org.apache.spark.ml.dsl.utils.FlowUtils
import org.apache.spark.storage.BlockManagerId
import org.slf4j.LoggerFactory

import scala.concurrent.TimeoutException
import scala.reflect.ClassTag
import scala.util.Random

abstract class CommonUtils {

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
  def retry = RetryFixedInterval

  protected def _callerShowStr = {
    val result = FlowUtils.callerShowStr(
      exclude = Seq(classOf[CommonUtils])
    )
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
        "rootkey.csv"
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
}

object CommonUtils extends CommonUtils
