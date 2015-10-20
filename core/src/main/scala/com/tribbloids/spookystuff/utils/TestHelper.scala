package com.tribbloids.spookystuff.utils

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

/**
 * Created by peng on 18/10/15.
 */
object TestHelper {

  val processors: Int = Runtime.getRuntime.availableProcessors()

  //if SPARK_PATH is detected, use local-cluster simulation mode
  //otherwise use local mode
  val testSparkConf: SparkConf = {

    //always use KryoSerializer, it is less stable than Native Serializer
    val conf: SparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.tribbloids.spookystuff.SpookyRegistrator")
      .set("spark.kryoserializer.buffer.max", "512m")

    val sparkHome = System.getenv("SPARK_HOME") //TODO: add more condition to force local mode?
    if (sparkHome == null) {
//      if (true) {
      LoggerFactory.getLogger(this.getClass).info("initialization Spark Context in local mode")
      conf.setMaster(s"local[$processors,4]")
    }
    else {
      println("initialization Spark Context in local-cluster simulation mode")
      conf.setMaster(s"local-cluster[1,$processors,1024]")
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

  val tempPath = System.getProperty("user.dir") + "/temp/"

  def clearTempDir(): Unit = {
    val file = new File(tempPath) //TODO: clean up S3 as well

    FileUtils.deleteDirectory(file)
  }
}
