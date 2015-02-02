package org.tribbloid.spookystuff.integration

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.tribbloid.spookystuff.dsl.{Minimal, Smart, QueryOptimizer}
import org.tribbloid.spookystuff.utils.Utils
import org.tribbloid.spookystuff.{DirConf, SpookyContext}

/**
 * Created by peng on 12/2/14.
 */
abstract class IntegrationSuite extends FunSuite with BeforeAndAfterAll {

  import org.scalatest.Matchers._

import scala.concurrent.duration._

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

  val localDirs = DirConf(root = "file://"+System.getProperty("user.home")+"/spooky-integration/")
  val s3Dirs = DirConf(root = "s3n://spooky-integration/")

  lazy val localCacheEnv = new SpookyContext(
    sql,
    localDirs,
    autoSave = false
  )

  lazy val s3CacheEnv = new SpookyContext(
    sql,
    s3Dirs,
    autoSave = false
  )

  private def assertBeforeCache(spooky: SpookyContext): Unit = {
    val metrics = spooky.metrics

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numPages(spooky.defaultQueryOptimizer))
    assert(metrics.pagesFetchedFromWeb.value === pageFetched)
    assert(metrics.pagesFetchedFromCache.value === 0)
    assert(metrics.sessionInitialized.value === numSessions(spooky.defaultQueryOptimizer) +- 1)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === numDrivers(spooky.defaultQueryOptimizer) +- 1)
    assert(metrics.driverReclaimed.value >= metrics.driverInitialized.value)
  }

  private def assertAfterCache(spooky: SpookyContext): Unit = {
    val metrics = spooky.metrics

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numPages(spooky.defaultQueryOptimizer))
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
      spooky.cacheRead = false
      spooky.cleanMetrics()
      doMain(spooky)
      assertBeforeCache(spooky)
    }

    Utils.retry(2) {
      spooky.cacheRead = true
      spooky.cleanMetrics()
      doMain(spooky)
      assertAfterCache(spooky)
    }
  }

  test("local cache, smart optimizer") {

    localCacheEnv.defaultQueryOptimizer = Smart
    doTest(localCacheEnv)
  }

  test("s3 cache, smart optimizer") {

    localCacheEnv.defaultQueryOptimizer = Smart
    doTest(s3CacheEnv)
  }

  test("local cache, minimal optimizer") {

    localCacheEnv.defaultQueryOptimizer = Minimal
    doTest(localCacheEnv)
  }

  test("s3 cache, minimal optimizer") {

    s3CacheEnv.defaultQueryOptimizer = Minimal
    doTest(s3CacheEnv)
  }

  def doMain(spooky: SpookyContext): Unit

  def numPages: QueryOptimizer => Int

  def numSessions: QueryOptimizer => Int = numPages

  def numDrivers: QueryOptimizer => Int = numSessions
}