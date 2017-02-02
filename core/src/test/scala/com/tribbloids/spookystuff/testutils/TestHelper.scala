package com.tribbloids.spookystuff.testutils

import java.io.File
import java.util.Properties

import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkException}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class TestHelper() {

  val TEMP_PATH = SpookyUtils.\\\(System.getProperty("user.dir"), "temp")
  val UNPACK_RESOURCE_PATH = SpookyUtils.\\\(System.getProperty("java.io.tmpdir"), "spookystuff", "resources")

  val props = new Properties()
  try {
    props.load(ClassLoader.getSystemResourceAsStream("rootkey.csv"))
  }
  catch {
    case e: Throwable =>
      println("rootkey.csv is missing")
  }

  val S3Path = Option(props.getProperty("S3Path"))
  if (S3Path.isDefined) println("Test on AWS S3 with credentials provided by rootkey.csv")

  val AWSAccessKeyId = Option(props.getProperty("AWSAccessKeyId"))
  val AWSSecretKey = Option(props.getProperty("AWSSecretKey"))
  AWSAccessKeyId.foreach{
    System.setProperty("fs.s3.awsAccessKeyId", _) //TODO: useless here? set in conf directly?
  }
  AWSSecretKey.foreach{
    System.setProperty("fs.s3.awsSecretAccessKey", _)
  }

  def sparkHome = System.getenv("SPARK_HOME")

  val numCores: Int = Runtime.getRuntime.availableProcessors()

  lazy val clusterSize_numCoresPerWorker_Opt: Option[(Int, Int)] = {
    Option(sparkHome).flatMap {
      h =>
        (
          Option(props.getProperty("ClusterSize")).map(_.toInt),
          Option(props.getProperty("NumCoresPerWorker")).map(_.toInt)
        ) match {
          case (None, None) =>
            None
          case (Some(v1), Some(v2)) =>
            Some(v1 -> v2)
          case (Some(v1), None) =>
            Some(v1 -> Math.max(numCores / v1, 1))
          case (None, Some(v2)) =>
            Some(Math.max(numCores / v2, 1), v2)
        }
    }
  }

  //only used in local mode
  lazy val maxLocalCores = Option(props.getProperty("maxLocalCores")).map(_.toInt)

  def clusterSizeOpt: Option[Int] = clusterSize_numCoresPerWorker_Opt.map(_._1)
  def numCoresPerWorkerOpt: Option[Int] = clusterSize_numCoresPerWorker_Opt.map(_._2)

  def maxFailures: Int = {
    Option(props.getProperty("MaxFailures")).map(_.toInt).getOrElse(4)
  }

  def numWorkers = clusterSizeOpt.getOrElse(1)
  def numComputers = clusterSizeOpt.map(_ + 1).getOrElse(1)

  //if SPARK_PATH & ClusterSize in rootkey.csv is detected, use local-cluster simulation mode
  //otherwise use local mode
  val TestSparkConf: SparkConf = {

    //always use KryoSerializer, it is less stable than Native Serializer
    val conf: SparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .set("spark.kryo.registrator", "com.tribbloids.spookystuff.SpookyRegistrator")Incomplete for the moment
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("dummy.property", "dummy")

    conf.setAll(coreSettings)

    Option(System.getProperty("fs.s3.awsAccessKeyId")).foreach {
      v =>
        conf.set("spark.hadoop.fs.s3.awsAccessKeyId", v)
        conf.set("spark.hadoop.fs.s3n.awsAccessKeyId", v)
        conf.set("spark.hadoop.fs.s3a.awsAccessKeyId", v)
    }
    Option(System.getProperty("fs.s3.awsSecretAccessKey")).foreach {
      v =>
        conf.set("spark.hadoop.fs.s3.awsSecretAccessKey", v)
        conf.set("spark.hadoop.fs.s3n.awsSecretAccessKey", v)
        conf.set("spark.hadoop.fs.s3a.awsSecretAccessKey", v)
    }

    conf.setAppName("Test")
    conf
  }

  final val TOTAL_MEMORY_CAP = 32768
  final private def executorMemoryCapOpt = clusterSizeOpt.map(v => TOTAL_MEMORY_CAP / v)
  def executorMemoryOpt: Option[Int] = for (n <- numCoresPerWorkerOpt; c <- executorMemoryCapOpt) yield {
    Math.min(n*1024, c)
  }

  /**
    * @return local mode: None -> local[n, 4]
    *         cluster simulation mode: Some(SPARK_HOME) -> local-cluster[m,n, mem]
    */
  lazy val coreSettings: Map[String, String] = {
    if (clusterSizeOpt.isEmpty || numCoresPerWorkerOpt.isEmpty) {
      val masterStr = s"local[${(Seq(numCores) ++ maxLocalCores.toSeq).min},$maxFailures]"
      println("initializing SparkContext in local mode:" + masterStr)
      Map(
        "spark.master" -> masterStr
      )
    }
    else {
      val masterStr = s"local-cluster[${clusterSizeOpt.get},${numCoresPerWorkerOpt.get},${executorMemoryOpt.get + 256}]"
      println(s"initializing SparkContext in local-cluster simulation mode:" + masterStr)
      Map(
        "spark.master" -> masterStr,
        "spark.home" -> sparkHome,
        "spark.executor.memory" -> (executorMemoryOpt.get + "m"),
        "spark.driver.extraClassPath" -> sys.props("java.class.path"),
        "spark.executor.extraClassPath" -> sys.props("java.class.path"),
        "spark.task.maxFailures" -> maxFailures.toString
      )
    }
  }

  lazy val TestSpark = {
    val sc = SparkContext.getOrCreate(TestSparkConf)
    sys.addShutdownHook {

      println("=============== Stopping Test Spark Context ==============")
      // Suppress the following log error when shutting down in local-cluster mode:
      // Remote RPC client disassociated. Likely due to containers exceeding thresholds, or network issues.
      // Check driver logs for WARN messages.
      // java.lang.IllegalStateException: Shutdown hooks cannot be modified during shutdown
      val logger = org.apache.log4j.Logger.getRootLogger
      //      val oldLevel = logger.getLevel
      logger.setLevel(org.apache.log4j.Level.toLevel("OFF"))
      //      try {
      //        sc.stop()
      //      }
      //      catch {
      //        case e: Throwable =>
      //          println(e)
      //          e.printStackTrace()
      //      }
      //      finally {
      //        logger.setLevel(oldLevel)
      //      }
      //      println("=============== Test Spark Context has stopped ==============")
    }
    sc
  }
  lazy val TestSQL = SQLContext.getOrCreate(TestSpark)

  def setLoggerDuring[T](clazzes: Class[_]*)(fn: =>T, level: String = "OFF"): T = {
    val logger_oldLevels = clazzes.map {
      clazz =>
        val logger = org.apache.log4j.Logger.getLogger(clazz)
        val oldLevel = logger.getLevel
        logger.setLevel(org.apache.log4j.Level.toLevel(level))
        logger -> oldLevel
    }
    try {
      fn
    }
    finally {
      logger_oldLevels.foreach {
        case (logger, oldLevel) =>
          logger.setLevel(oldLevel)
      }
    }
  }

  def assureKryoSerializer(sc: SparkContext): Unit = {
    val ser = SparkEnv.get.serializer
    require(ser.isInstanceOf[KryoSerializer])

    val rdd = sc.parallelize(Seq(sc)).map {
      v =>
        v.startTime
    }

    try {
      rdd.reduce(_ + _)
      throw new AssertionError("should throw SparkException")
    }
    catch {
      case e: SparkException =>
        val ee = e
        assert(
          ee.getMessage.contains("com.esotericsoftware.kryo.KryoException"),
          "should be triggered by KryoException, but the message doesn't indicate that:\n"+ ee.getMessage
        )
      case e: Throwable =>
        throw new AssertionError(s"Expecting SparkException, but ${e.getClass.getSimpleName} was thrown", e)
    }
  }

  //TODO: clean up S3 as well
  def clearTempDirs(paths: Seq[String] = Seq(TEMP_PATH)): Unit = {
    paths.foreach {
      path =>
        Try {
          val file = new File(path)
          SpookyUtils.retry(3) {
            FileUtils.deleteDirectory(file)
          }
        }
    }
  }

  def timer[T](fn: => T): (T, Long) = {
    val startTime = System.currentTimeMillis()
    val result = fn
    val endTime = System.currentTimeMillis()
    (result, endTime - startTime)
  }

  def assert(fn: =>Boolean) = Predef.assert(fn)
  def assert(fn: =>Boolean, message: =>Any) = Predef.assert(fn, message)
  def intercept[EE <: Exception: ClassTag](fn: =>Any) = {
    val trial = Try {
      fn
    }
    val expectedErrorName = implicitly[ClassTag[EE]].runtimeClass.getSimpleName
    trial match {
      case Failure(e: EE) =>
      case Failure(e) =>
        throw new AssertionError(s"Expecting $expectedErrorName, but get ${e.getClass.getSimpleName}", e)
      case Success(n) =>
        throw new AssertionError(s"expecting $expectedErrorName, but no exception was thrown")
    }
  }

  //  override def finalize(): Unit = {
  //    if (testSparkCreated) {
  //      println("=============== Stopping Test Spark Context ==============")
  //      try {
  //        TestSpark.stop()
  //      }
  //      catch {
  //        case e: Throwable =>
  //          println(e)
  //          e.printStackTrace()
  //      }
  //      println("=============== Test Spark Context has stopped ==============")
  //    }
  //
  //    super.finalize()
  //  }
}

object TestHelper extends TestHelper()

object SparkRunnerHelper extends TestHelper() {

  override lazy val clusterSizeOpt: Option[Int] = None
}