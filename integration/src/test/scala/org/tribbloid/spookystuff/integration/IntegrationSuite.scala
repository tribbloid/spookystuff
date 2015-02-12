package org.tribbloid.spookystuff.integration

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.tribbloid.spookystuff.SpookyConf.Dirs
import org.tribbloid.spookystuff.dsl.{Wide, Narrow, QueryOptimizer, WideLookup}
import org.tribbloid.spookystuff.utils.Utils
import org.tribbloid.spookystuff.{SpookyConf, SpookyContext}

/**
 * Created by peng on 12/2/14.
 */
abstract class IntegrationSuite extends FunSuite with BeforeAndAfterAll {

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
      sharedMetrics = true,
      autoSave = false,
      checkpointInterval = 2,
      batchSize = 2
    )
  )

  lazy val s3CacheEnv = new SpookyContext(
    sql,
    new SpookyConf(
      s3Dirs,
      sharedMetrics = true,
      autoSave = false,
      checkpointInterval = 2,
      batchSize = 2
    )
  )

  private def assertBeforeCache(spooky: SpookyContext): Unit = {
    val metrics = spooky.metrics

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numPages(spooky.conf.defaultQueryOptimizer))
    assert(metrics.pagesFetchedFromWeb.value === pageFetched)
    assert(metrics.pagesFetchedFromCache.value === 0)
    assert(metrics.sessionInitialized.value === numSessions(spooky.conf.defaultQueryOptimizer))
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === numDrivers(spooky.conf.defaultQueryOptimizer))
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

  private val retry = 2

  private def doTest(spooky: SpookyContext): Unit ={

    Utils.retry(retry) { //sometimes accumulator missed a few for no reason
      spooky.conf.cacheRead = false
      spooky.zeroIn()
      doMain(spooky)
      assertBeforeCache(spooky)
    }

    Utils.retry(retry) {
      spooky.conf.cacheRead = true
      spooky.zeroIn()
      doMain(spooky)
      assertAfterCache(spooky)
    }
  }

  test("local cache, wide-lookup optimizer") {

    localCacheEnv.conf.defaultQueryOptimizer = WideLookup
    doTest(localCacheEnv)
  }

  test("s3 cache, wide-lookup optimizer") {

    s3CacheEnv.conf.defaultQueryOptimizer = WideLookup
    doTest(s3CacheEnv)
  }

//  test("local cache, wide (no lookup) optimizer") {
//
//    localCacheEnv.conf.defaultQueryOptimizer = Wide
//    doTest(localCacheEnv)
//  }
//
//  test("s3 cache, wide (no lookup) optimizer") {
//
//    localCacheEnv.conf.defaultQueryOptimizer = Wide
//    doTest(s3CacheEnv)
//  }

  test("local cache, narrow optimizer") {

    localCacheEnv.conf.defaultQueryOptimizer = Narrow
    doTest(localCacheEnv)
  }

  test("s3 cache, narrow optimizer") {

    s3CacheEnv.conf.defaultQueryOptimizer = Narrow
    doTest(s3CacheEnv)
  }

  def doMain(spooky: SpookyContext): Unit

  def numPages: QueryOptimizer => Int

  def numSessions: QueryOptimizer => Int = numPages

  def numDrivers: QueryOptimizer => Int = numSessions
}