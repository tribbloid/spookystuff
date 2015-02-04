package org.tribbloid.spookystuff.integration

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.tribbloid.spookystuff.SpookyConf.Dirs
import org.tribbloid.spookystuff.{SpookyConf, SpookyContext}
import org.tribbloid.spookystuff.dsl.{Minimal, QueryOptimizer, Smart}
import org.tribbloid.spookystuff.utils.Utils

/**
 * Created by peng on 12/2/14.
 */
abstract class IntegrationSuite extends FunSuite with BeforeAndAfterAll {

  import org.scalatest.Matchers._

  @transient var sc: SparkContext = _
  @transient var sql: SQLContext = _

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

    sql = new SQLContext(sc)

    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

  val localDirs = new Dirs(root = "file://"+System.getProperty("user.home")+"/spooky-integration/")
  val s3Dirs = new Dirs(root = "s3n://spooky-integration/")

  lazy val localCacheEnv = new SpookyContext(
    sql,
    new SpookyConf(
      localDirs,
      autoSave = false
    )
  )

  lazy val s3CacheEnv = new SpookyContext(
    sql,
    new SpookyConf(
      s3Dirs,
      autoSave = false
    )
  )

  private def assertBeforeCache(spooky: SpookyContext): Unit = {
    val metrics = spooky.metrics

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numPages(spooky.conf.defaultQueryOptimizer))
    assert(metrics.pagesFetchedFromWeb.value === pageFetched)
    assert(metrics.pagesFetchedFromCache.value === 0)
    assert(metrics.sessionInitialized.value === numSessions(spooky.conf.defaultQueryOptimizer) +- 1)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === numDrivers(spooky.conf.defaultQueryOptimizer) +- 1)
    assert(metrics.driverReclaimed.value >= metrics.driverInitialized.value)
  }

  private def assertAfterCache(spooky: SpookyContext): Unit = {
    val metrics = spooky.metrics

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numPages(spooky.conf.defaultQueryOptimizer))
    assert(metrics.pagesFetchedFromWeb.value === 0)
    assert(metrics.pagesFetchedFromCache.value === pageFetched)
    assert(metrics.sessionInitialized.value === 0)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === 0)
    assert(metrics.driverReclaimed.value >= metrics.driverInitialized.value)
    assert(metrics.DFSReadSuccess.value > 0)
    assert(metrics.DFSReadFail.value === 0)
  }

  private def doTest(spooky: SpookyContext): Unit ={

    Utils.retry(2) { //sometimes accumulator missed a few for no reason
      spooky.conf.cacheRead = false
      spooky.zeroIn()
      doMain(spooky)
      assertBeforeCache(spooky)
    }

    Utils.retry(2) {
      spooky.conf.cacheRead = true
      spooky.zeroIn()
      doMain(spooky)
      assertAfterCache(spooky)
    }
  }

//  test("local cache, smart optimizer") {
//
//    localCacheEnv.conf.defaultQueryOptimizer = Smart
//    doTest(localCacheEnv)
//  }

  test("s3 cache, smart optimizer") {

    localCacheEnv.conf.defaultQueryOptimizer = Smart
    doTest(s3CacheEnv)
  }

//  test("local cache, minimal optimizer") {
//
//    localCacheEnv.conf.defaultQueryOptimizer = Minimal
//    doTest(localCacheEnv)
//  }

  test("s3 cache, minimal optimizer") {

    s3CacheEnv.conf.defaultQueryOptimizer = Minimal
    doTest(s3CacheEnv)
  }

  def doMain(spooky: SpookyContext): Unit

  def numPages: QueryOptimizer => Int

  def numSessions: QueryOptimizer => Int = numPages

  def numDrivers: QueryOptimizer => Int = numSessions
}