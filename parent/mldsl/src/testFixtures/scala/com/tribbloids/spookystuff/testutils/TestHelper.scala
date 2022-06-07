package com.tribbloids.spookystuff.testutils

import com.tribbloids.spookystuff.utils.classpath.ClasspathResolver
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.lifespan.LocalCleanable
import com.tribbloids.spookystuff.utils.{CommonConst, CommonUtils, ConfUtils}
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkException}
import org.slf4j.LoggerFactory

import java.io.File
import java.util.{Date, Properties}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

abstract class TestHelper extends LocalCleanable {

  val properties: Properties = {

    val properties = new Properties()

    Try {
      properties.load(ClassLoader.getSystemResourceAsStream(".rootkey.csv"))
    }.recoverWith {
      case _: Exception =>
        Try {
          properties.load(ClassLoader.getSystemResourceAsStream("rootkey.csv"))
        }
    }.getOrElse {
      println("rootkey.csv is missing")
    }

    properties
  }

  def getProperty(key: String): Option[String] = {

    val result = Option(properties.getProperty(key))
    result match {
      case Some(v) =>
        println(s"[Test property] $key \t -> $v")
      case None =>
        println(s"[Test property] $key \t is undefined")
    }
    result

  }

  val S3Path: Option[String] = getProperty("S3Path")

  val AWSAccessKeyId: Option[String] = getProperty("AWSAccessKeyId")
  val AWSSecretKey: Option[String] = getProperty("AWSSecretKey")

  def SPARK_HOME: String = System.getenv("SPARK_HOME")

  final val MAX_TOTAL_MEMORY = 8 * 1024
  final val MEMORY_PER_CORE = 1024
  //  final val EXECUTOR_JVM_MEMORY_OVERHEAD = 256 //TODO: remove, too complex

  final val MAX_CORES = MAX_TOTAL_MEMORY / MEMORY_PER_CORE

  val METASTORE_PATH: String = CommonUtils.\\\(CommonConst.USER_DIR, "metastore_db")
  val WAREHOUSE_PATH: String = CommonUtils.\\\(CommonConst.USER_DIR, "warehouse")

  @transient var sparkSessionInitialised: Boolean = false

  {
    val report: String = ClasspathResolver.withOverview { o =>
      o.completeReport
    }

    if (S3Path.isDefined) println("Test on AWS S3 with credentials provided by rootkey.csv")

    Seq("s3,s3n,s3a").foreach { n =>
      AWSAccessKeyId.foreach {
        System.setProperty(s"spark.hadoop.fs.$n.awsAccessKeyId", _) // TODO: useless here? set in conf directly?
      }
      AWSSecretKey.foreach {
        System.setProperty(s"spark.hadoop.fs.$n.awsSecretAccessKey", _)
      }
    }

    cleanBeforeAndAfter()
  }

  override def _lifespan: Lifespan = Lifespan.HadoopShutdown.BeforeSpark()

  override def cleanImpl(): Unit = {

    println("=============== Stopping Test Spark Context ==============")
    // Suppress the following log error when shutting down in local-cluster mode:
    // Remote RPC client disassociated. Likely due to containers exceeding thresholds, or network issues.
    // Check driver logs for WARN messages.
    // java.lang.IllegalStateException: Shutdown hooks cannot be modified during shutdown
    val logger = org.apache.log4j.Logger.getRootLogger
    logger.setLevel(org.apache.log4j.Level.toLevel("OFF"))

    TestSC.stop()

    //      println("=============== Test Spark Context has stopped ==============")
    cleanBeforeAndAfter()
  }

  def cleanBeforeAndAfter(): Unit = {
    cleanTempDirs(
      Seq(
        WAREHOUSE_PATH,
        METASTORE_PATH,
        CommonConst.USER_TEMP_DIR
      )
    )
  }

  lazy val numCores: Int = {
    val cap = getProperty("MaxCores")
      .map(_.toInt)
      .getOrElse(MAX_CORES)
    var n = Math.min(
      Runtime.getRuntime.availableProcessors(),
      cap
    )

    if (n < 2) n = 2
    n
  }

  lazy val clusterSize_numCoresPerWorker_Opt: Option[(Int, Int)] = {

    val tuple = (
      getProperty("ClusterSize").map(_.toInt),
      getProperty("NumCoresPerWorker").map(_.toInt)
    )

    Option(SPARK_HOME)
      .flatMap { _ =>
        tuple match {
          case (None, None) =>
            None
          case (Some(v1), Some(v2)) =>
            Some(v1 -> v2)
          case (Some(v1), None) =>
            Some(v1 -> Math.max(numCores / v1, 1))
          case (None, Some(v2)) =>
            Some(Math.max(numCores / v2, 1) -> v2)
        }
      }
      .orElse {
        tuple match {
          case (None, None) =>
          case _ =>
            LoggerFactory
              .getLogger(this.getClass)
              .warn("cannot use cluster mode as SPARK_HOME is missing")
        }
        None
      }
  }

  def clusterSizeOpt: Option[Int] = clusterSize_numCoresPerWorker_Opt.map(_._1)
  def numCoresPerWorkerOpt: Option[Int] = clusterSize_numCoresPerWorker_Opt.map(_._2)

  def maxFailures: Int = {
    getProperty("MaxFailures").map(_.toInt).getOrElse(4)
  }

  def numWorkers: Int = clusterSizeOpt.getOrElse(1)
  def numComputers: Int = clusterSizeOpt.map(_ + 1).getOrElse(1)

  def executorMemoryCapOpt: Option[Int] = clusterSizeOpt.map(clusterSize => MAX_TOTAL_MEMORY / clusterSize)

  def executorMemoryOpt: Option[Int] = for (n <- numCoresPerWorkerOpt; cap <- executorMemoryCapOpt) yield {
    Math.min(n * MEMORY_PER_CORE, cap)
  }

  @transient lazy val envOverrides: Map[String, String] = Map(
    "SPARK_SCALA_VERSION" -> CommonUtils.scalaBinaryVersion
    //    "SPARK_LOCAL_HOSTNAME" -> "localhost"
  )

  case object CoreSettings {

    lazy val masterEnv: String = System.getenv("SPARK_MASTER")

    lazy val isLocal: Boolean = clusterSizeOpt.isEmpty || numCoresPerWorkerOpt.isEmpty

    lazy val masterStr: String = if (masterEnv != null) {
      masterEnv
    } else if (isLocal) {
      val masterStr = s"local[$numCores,$maxFailures]"
      println("initializing SparkContext in local mode:" + masterStr)
      masterStr
    } else {

      if (envOverrides.nonEmpty) {
        LoggerFactory
          .getLogger(this.getClass)
          .warn(
            "overriding system variables ... this may be unstable for some JVM"
          )

        envOverrides.foreach {
          case (k, v) =>
            ConfUtils.overrideEnv(k, v)
        }
      }

      val masterStr =
        s"local-cluster[${clusterSizeOpt.get},${numCoresPerWorkerOpt.get},${executorMemoryOpt.get}]"
      println(s"initializing SparkContext in local-cluster simulation mode:" + masterStr)

      masterStr
    }

    /**
      * @return
      *   local mode: None -> local[n, 4] cluster simulation mode: Some(SPARK_HOME) -> local-cluster[m,n, mem]
      */
    lazy val asMap: Map[String, String] = {

      val base1 = Map(
        "spark.master" -> masterStr,
        "spark.task.maxFailures" -> maxFailures.toString
      )

      val base2 = if (CoreSettings.isLocal) {
        base1
      } else {
        base1 ++ Map(
          "spark.home" -> SPARK_HOME,
          //        "spark.executor.memory" -> (executorMemoryOpt.get + "m"),
          "spark.driver.extraClassPath" -> sys.props("java.class.path"),
          "spark.executor.extraClassPath" -> sys.props("java.class.path")
        )
      }

      base2 ++ Map(
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
        //      .set("spark.kryo.registrator", "com.tribbloids.spookystuff.SpookyRegistrator")Incomplete for the moment
        "spark.kryoserializer.buffer.max" -> "512m",
        "spark.sql.warehouse.dir" -> WAREHOUSE_PATH,
        //      "hive.metastore.warehouse.dir" -> WAREHOUSE_PATH,
        "dummy.property" -> "dummy"
      )
    }
  }

  // if SPARK_PATH & ClusterSize in rootkey.csv is detected, use local-cluster simulation mode
  // otherwise use local mode
  lazy val TestSparkConf: SparkConf = {

    // always use KryoSerializer, it is less stable than Native Serializer
    val conf: SparkConf = new SparkConf(false)

    conf.setAll(CoreSettings.asMap)

    // TODO: remove, should be set by users
//    Option(System.getProperty("fs.s3.awsAccessKeyId")).foreach { v =>
//      conf.set("spark.hadoop.fs.s3.awsAccessKeyId", v)
//      conf.set("spark.hadoop.fs.s3n.awsAccessKeyId", v)
//      conf.set("spark.hadoop.fs.s3a.awsAccessKeyId", v)
//    }
//    Option(System.getProperty("fs.s3.awsSecretAccessKey")).foreach { v =>
//      conf.set("spark.hadoop.fs.s3.awsSecretAccessKey", v)
//      conf.set("spark.hadoop.fs.s3n.awsSecretAccessKey", v)
//      conf.set("spark.hadoop.fs.s3a.awsSecretAccessKey", v)
//    }

    conf.setAppName("Test")

    println(
      s"""
         |------------------------------------------
         |deriving SparkConf for testing ...
         |------------------------------------------
         |${conf.toDebugString}
         |==========================================
         |""".stripMargin
    )

    conf
  }

  lazy val TestSparkSession: SparkSession = {

    require(lifespan.ctx.taskAttemptID.isEmpty, "SparkSession cannot be initialised on executor")

    val builder = SparkSession.builder
      .config(TestSparkConf)

    Try {
      builder.enableHiveSupport()
    }

    val session = builder.getOrCreate()
    val sc = session.sparkContext

    CommonUtils.retry(3, 8000, silent = true) {
      // wait for all executors in local-cluster mode to be online
      require(
        sc.defaultParallelism == numCores, {
          val info = s"waiting for $numCores cores, only ${sc.defaultParallelism} are registered:\n" +
            getExecutorSummaryText(sc)
          LoggerFactory.getLogger(this.getClass).warn(info)
          "timeout: " + info
        }
      )
    }

    sparkSessionInitialised = true

    assureKryoSerializer(sc)
    session
  }

  def getExecutorSummaryText(sc: SparkContext): String = {
    val executors = sc.getExecutorMemoryStatus
    val executorReport = executors
      .map {
        case (k, v) =>
          s"$k :\t ${v._1} bytes available / ${v._2} bytes remaining"
      }
      .mkString("\n")

    s"""
       |Executors:
       |$executorReport
       |""".trim.stripMargin
  }

  lazy val TestSC: SparkContext = TestSparkSession.sparkContext
  lazy val TestSQL: SQLContext = TestSparkSession.sqlContext

  lazy val enableCheckpoint: Unit = {

    TestSC.setCheckpointDir("/tmp/spark-checkpoint/" + new Date().toString)
  }

  def setLoggerDuring[T](clazzes: Class[_]*)(fn: => T, level: String = "OFF"): T = {
    val logger_oldLevels = clazzes.map { clazz =>
      val logger = org.apache.log4j.Logger.getLogger(clazz)
      val oldLevel = logger.getLevel
      logger.setLevel(org.apache.log4j.Level.toLevel(level))
      logger -> oldLevel
    }
    try {
      fn
    } finally {
      logger_oldLevels.foreach {
        case (logger, oldLevel) =>
          logger.setLevel(oldLevel)
      }
    }
  }

  def assureKryoSerializer(sc: SparkContext, rigorous: Boolean = false): Unit = {
    val ser = SparkEnv.get.serializer
    require(ser.isInstanceOf[KryoSerializer])

    // will print a long warning message into stderr, disabled by default
    if (rigorous) {
      val rdd = sc.parallelize(Seq(sc)).map { v =>
        v.startTime
      }

      try {
        rdd.reduce(_ + _)
        throw new AssertionError("should throw SparkException")
      } catch {
        case e: SparkException =>
          val ee = e
          Predef.assert(
            ee.getMessage.contains("com.esotericsoftware.kryo.KryoException"),
            "should be triggered by KryoException, but the message doesn't indicate that:\n" + ee.getMessage
          )
        case e: Exception =>
          throw new AssertionError(s"Expecting SparkException, but ${e.getClass.getSimpleName} was thrown", e)
      }
    }
  }

  // TODO: clean up S3 as well
  // also, this should become a class that inherit cleanable
  def cleanTempDirs(paths: Seq[String] = Seq(CommonConst.USER_TEMP_DIR)): Unit = {
    paths.filter(_ != null).foreach { path =>
      try {
        val file = new File(path)
        CommonUtils.retry(3) {
          val absoluteFile = file.getAbsoluteFile
          if (absoluteFile != null && absoluteFile.exists())
            FileUtil.fullyDelete(absoluteFile, true)
        }
      } catch {
        case e: Throwable =>
          LoggerFactory.getLogger(this.getClass).warn("cannot clean tempDirs", e)
      }
    }
  }

  def intercept[EE <: Exception: ClassTag](fn: => Any): Unit = {
    val trial = Try {
      fn
    }
    val expectedErrorName = implicitly[ClassTag[EE]].runtimeClass.getSimpleName
    trial match {
      case Failure(_: EE) =>
      case Failure(e) =>
        throw new AssertionError(s"Expecting $expectedErrorName, but get ${e.getClass.getSimpleName}", e)
      case Success(_) =>
        throw new AssertionError(s"expecting $expectedErrorName, but no exception was thrown")
    }
  }
}

object TestHelper extends TestHelper() {}
