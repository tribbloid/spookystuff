package org.tribbloid.spookystuff.integration

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite, Tag}
import org.tribbloid.spookystuff.SpookyContext

import scala.concurrent.duration

/**
 * Created by peng on 12/2/14.
 */
abstract class IntegrationSuite extends FunSuite with BeforeAndAfter {

  import duration._

  object Integration extends Tag("Integration")

  val appName = this.getClass.getSimpleName.replace("$","")
  val conf: SparkConf = new SparkConf().setAppName(appName)
    .setMaster("local[*]")

  val sc: SparkContext = {
    val prop = new Properties()
    prop.load(ClassLoader.getSystemResourceAsStream("rootkey.csv"))
    val AWSAccessKeyId = prop.getProperty("AWSAccessKeyId")
    val AWSSecretKey = prop.getProperty("AWSSecretKey")

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration
      .set("fs.s3n.awsAccessKeyId", AWSAccessKeyId)
    sc.hadoopConfiguration
      .set("fs.s3n.awsSecretAccessKey", AWSSecretKey)

    sc
  }
  val sql: SQLContext = new SQLContext(sc)

  lazy val noCacheReadEnv = {
    val spooky: SpookyContext = new SpookyContext(sql)
    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-integration/")
    spooky.pageExpireAfter = 0.seconds
    spooky
  }

  lazy val localCacheEnv = {
    val spooky: SpookyContext = new SpookyContext(sql)
    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-integration/")
    spooky.pageExpireAfter = 10.minutes
    spooky
  }

  override def finalize(){
    sc.stop()
  }

  test("local cache", Integration) {

    doMain(noCacheReadEnv)

    assert(noCacheReadEnv.metrics.pagesFetched.value === expectedPages)
    assert(noCacheReadEnv.metrics.pagesFetchedFromCache.value === 0)

    doMain(localCacheEnv)

    assert(localCacheEnv.metrics.pagesFetched.value === expectedPages)
    assert(localCacheEnv.metrics.pagesFetchedFromCache.value === expectedPages)
    assert(localCacheEnv.metrics.driverInitialized.value === 0)
    assert(localCacheEnv.metrics.DFSReadSuccess.value > 0)
    assert(localCacheEnv.metrics.DFSReadFail.value === 0)
  }

  def doMain(spooky: SpookyContext): Unit

  def expectedPages: Int
}