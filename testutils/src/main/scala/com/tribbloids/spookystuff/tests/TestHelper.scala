package com.tribbloids.spookystuff.tests

import java.io.File
import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkException}
import org.slf4j.LoggerFactory

object TestHelper {

  val numProcessors: Int = Runtime.getRuntime.availableProcessors()

  val tempPath = System.getProperty("user.dir") + "/temp/"

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

  val clusterSize = Option(props.getProperty("ClusterSize")).map(_.toInt)

  //if SPARK_PATH & ClusterSize in rootkey.csv is detected, use local-cluster simulation mode
  //otherwise use local mode
  val TestSparkConf: SparkConf = {

    //always use KryoSerializer, it is less stable than Native Serializer
    val conf: SparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .set("spark.kryo.registrator", "com.tribbloids.spookystuff.SpookyRegistrator")Incomplete for the moment
      .set("spark.kryoserializer.buffer.max", "512m")

    val sparkHome = System.getenv("SPARK_HOME")
    if (sparkHome == null || clusterSize.isEmpty) {
      val masterStr = s"local[$numProcessors,4]"
      LoggerFactory.getLogger(this.getClass).info("initializing SparkContext in local mode:" + masterStr)
      conf.setMaster(masterStr)
    }
    else {
      val size = clusterSize.get
      val masterStr = s"local-cluster[$size,${numProcessors/size},1024]"
      println(s"initializing SparkContext in local-cluster simulation mode:" + masterStr)
      conf.setMaster(masterStr) //TODO: more than 1 nodes may cause some counters to have higher readings
    }

    //this is the only way to conduct local-cluster simulation
    if (conf.get("spark.master").contains("cluster")) {
      conf
        .setSparkHome(sparkHome)
        .set("spark.driver.extraClassPath", sys.props("java.class.path"))
        .set("spark.executor.extraClassPath", sys.props("java.class.path"))
    }

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
    val file = new File(tempPath) //TODO: clean up S3 as well

    FileUtils.deleteDirectory(file)
  }

  def timer[T](fn: => T): (T, Long) = {
    val startTime = System.currentTimeMillis()
    val result = fn
    val endTime = System.currentTimeMillis()
    (result, endTime - startTime)
  }

  val unpackedResourceRootPath = "target/resources/"
  def unpackResourceIfNotExist(resource: String): String = {
    val path = getClass.getClassLoader.getResource(resource)

    val fileOpt = {
      val file = FileUtils.toFile(path)
      if (file != null && file.exists()) Some(file)
      else None
    }
      .orElse {
        // avoid repeated unpacking of file.
        val file = new File(unpackedResourceRootPath + resource)
        if (file.exists()) Some(file)
        else None
      }

    fileOpt match {
      case Some(file) =>
        file.getAbsolutePath
      case None =>
        val srcURL= getClass.getClassLoader.getResource(resource)

        val targetFile = new File(unpackedResourceRootPath + resource)
        println(targetFile.getAbsolutePath)

        //        println("rsrc:" + resource)
        //        println("from:" + srcURL)
        //        println("to:  " + targetFile.getPath)

        //        try {
        FileUtils.copyURLToFile(srcURL, targetFile)
        //        }
        //        catch {
        //          case e: NullPointerException => //do nothing
        //        }

        for (i <- 0 to 50) {
          val targetFile = new File(unpackedResourceRootPath + resource)
          if (targetFile.exists()) return targetFile.getAbsolutePath
          println("file writing not finished ...")
          Thread.sleep(2000)
        }
        sys.error("file writing failed")
    }
  }
}
