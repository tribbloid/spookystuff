package com.tribbloids.spookystuff.tests

import java.io.File
import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
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
  val testSparkConf: SparkConf = {

    //always use KryoSerializer, it is less stable than Native Serializer
    val conf: SparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.tribbloids.spookystuff.SpookyRegistrator")
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

    conf
  }

  def clearTempDir(): Unit = {
    val file = new File(tempPath) //TODO: clean up S3 as well

    FileUtils.deleteDirectory(file)
  }
}
