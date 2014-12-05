package org.tribbloid.spookystuff

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import org.tribbloid.spookystuff.dsl.driverfactory.NaiveDriverFactory

/**
 * Created by peng on 11/30/14.
 */
abstract class SpookyEnvSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  var sc: SparkContext = _
  var sql: SQLContext = _
  var spooky: SpookyContext = _

  override def beforeAll() {
    val conf: SparkConf = new SparkConf().setAppName("integration")
      .setMaster("local[*]")

    val prop = new Properties()
    prop.load(ClassLoader.getSystemResourceAsStream("rootkey.csv"))
    val AWSAccessKeyId = prop.getProperty("AWSAccessKeyId")
    val AWSSecretKey = prop.getProperty("AWSSecretKey")

    sc = new SparkContext(conf)
    sc.hadoopConfiguration
      .set("fs.s3n.awsAccessKeyId", AWSAccessKeyId)
    sc.hadoopConfiguration
      .set("fs.s3n.awsSecretAccessKey", AWSSecretKey)

    val sql: SQLContext = new SQLContext(sc)

    spooky = new SpookyContext(
      sql,
      driverFactory = NaiveDriverFactory(loadImages = true)
    )

    spooky.autoSave = false
    spooky.autoCache = false
    spooky.autoRestore = false

    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")

    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

  before{
    spooky.autoSave = false
    spooky.autoCache = false
    spooky.autoRestore = false

    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")
  }
}
