package com.tribbloids.spookystuff.testutils

import java.io.File
import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkException}

object TestHelper {

  val numProcessors: Int = Runtime.getRuntime.availableProcessors()

  val TEMP_PATH = System.getProperty("user.dir") + "/temp/"
  val TARGET_PATH = System.getProperty("user.dir") + "/target/"

  val props = new Properties()
  try {
    props.load(ClassLoader.getSystemResourceAsStream("rootkey.csv"))
  }
  catch {
    case e: Throwable =>
      println("rootkey.csv is missing")
  }

  val S3Path = Option(props.getProperty("S3Path"))
  if (S3Path.isEmpty) println("Test on AWS S3 with credentials provided by rootkey.csv")

  val AWSAccessKeyId = Option(props.getProperty("AWSAccessKeyId"))
  val AWSSecretKey = Option(props.getProperty("AWSSecretKey"))
  AWSAccessKeyId.foreach{
    System.setProperty("fs.s3.awsAccessKeyId", _) //TODO: useless here? set in conf directly?
  }
  AWSSecretKey.foreach{
    System.setProperty("fs.s3.awsSecretAccessKey", _)
  }

  val clusterSizeOpt: Option[Int] = Option(props.getProperty("ClusterSize")).map(_.toInt)

  //if SPARK_PATH & ClusterSize in rootkey.csv is detected, use local-cluster simulation mode
  //otherwise use local mode
  val TestSparkConf: SparkConf = {

    //always use KryoSerializer, it is less stable than Native Serializer
    val conf: SparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .set("spark.kryo.registrator", "com.tribbloids.spookystuff.SpookyRegistrator")Incomplete for the moment
      .set("spark.kryoserializer.buffer.max", "512m")

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

  final val EXECUTOR_MEMORY = 2048

  /**
    *
    * @return local mode: None -> local[n, 4]
    *         cluster simulation mode: Some(SPARK_HOME) -> local-cluster[m,n, mem]
    */
  lazy val coreSettings: Map[String, String] = {
    val sparkHome = System.getenv("SPARK_HOME")
    if (sparkHome == null || clusterSizeOpt.isEmpty) {
      val masterStr = s"local[$numProcessors,4]"
      println("initializing SparkContext in local mode:" + masterStr)
      Map(
        "spark.master" -> masterStr
      )
    }
    else {
      val size = clusterSizeOpt.get
      val masterStr = s"local-cluster[$size,${numProcessors / size},${EXECUTOR_MEMORY + 512}]"
      println(s"initializing SparkContext in local-cluster simulation mode:" + masterStr)
      Map(
        "spark.master" -> masterStr,
        "spark.home" -> sparkHome,
        "spark.executor.memory" -> (EXECUTOR_MEMORY + "m"),
        "spark.driver.extraClassPath" -> sys.props("java.class.path"),
        "spark.executor.extraClassPath" -> sys.props("java.class.path")
      )
    }
  }

  def TestSpark = {
    val sc = SparkContext.getOrCreate(TestSparkConf)
    //TODO: Remote RPC client disassociated. Likely due to containers exceeding thresholds, or network issues. Check driver logs for WARN messages.
    //TODO: Cannot call methods on a stopped SparkContext. Disabled before solution is found
    //    Runtime.getRuntime.addShutdownHook(
    //      new Thread {
    //        override def run() = {
    //          println("=============== Stopping Spark Context ==============")
    //          sc.stop()
    //          println("=============== Test Spark Context has stopped ==============")
    //        }
    //      }
    //    )
    sc
  }
  def TestSQL = SQLContext.getOrCreate(TestSpark)

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

  def clearTempDir(): Unit = {
    val file = new File(TEMP_PATH) //TODO: clean up S3 as well

    FileUtils.deleteDirectory(file)
  }

  def timer[T](fn: => T): (T, Long) = {
    val startTime = System.currentTimeMillis()
    val result = fn
    val endTime = System.currentTimeMillis()
    (result, endTime - startTime)
  }
}
