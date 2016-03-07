package com.tribbloids.spookystuff.integration

import java.util.Date

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.utils.{TestHelper, Utils}
import com.tribbloids.spookystuff.{DirConf, SpookyConf, SpookyContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration

abstract class IntegrationSuite extends FunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _
  @transient var sql: SQLContext = _

  val phantomJS = DriverFactories.PhantomJS()
  val htmlUnit = DriverFactories.HtmlUnit()

  override def beforeAll() {
    val conf = TestHelper.testSparkConf.setAppName("integration")

    sc = new SparkContext(conf)
    sql = new SQLContext(sc)

    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }

    TestHelper.clearTempDir()
    super.afterAll()
  }

  lazy val roots: Seq[String] = {

    val local = Seq(TestHelper.tempPath + "spooky-integration/")

    local ++ TestHelper.S3Path
  }

  lazy val drivers = Seq(
    phantomJS
//    htmlUnit
  )

  lazy val optimizers = Seq(
    FetchOptimizers.Narrow,
    FetchOptimizers.Wide,
    FetchOptimizers.WebCacheAware
  )

  import duration._

  // testing matrix
  for (root <- roots) {
    for (driver <- drivers) {
      for (optimizer <- optimizers) {
        test(s"$optimizer/$driver/$root") {
          lazy val env = new SpookyContext(
            sql,
            new SpookyConf(
              new DirConf(root = root),
              driverFactory = driver,
              defaultFetchOptimizer = optimizer,
              shareMetrics = true,
//              checkpointInterval = 2,
              remoteResourceTimeout = 10.seconds
            )
          )

          doTest(env)
        }
      }
    }
  }

  //TODO: for local-cluster mode, some of these metrics may have higher than expected results because.
  def assertBeforeCache(spooky: SpookyContext): Unit = {
    val metrics = spooky.metrics
    println(metrics.toJSON)

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numFetchedPages(spooky.conf.defaultFetchOptimizer))
    assert(metrics.pagesFetchedFromRemote.value === numPagesDistinct)
    assert(metrics.pagesFetchedFromCache.value === pageFetched - numPagesDistinct)
    assert(metrics.sessionInitialized.value === numSessions)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === numDrivers)
    assert(metrics.driverReclaimed.value >= metrics.driverInitialized.value)
  }

  def assertAfterCache(spooky: SpookyContext): Unit = {
    val metrics = spooky.metrics
    println(metrics.toJSON)

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numFetchedPages(spooky.conf.defaultFetchOptimizer))
    assert(metrics.pagesFetchedFromRemote.value === 0)
    assert(metrics.pagesFetchedFromCache.value === pageFetched)
    assert(metrics.sessionInitialized.value === 0)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === 0)
    assert(metrics.driverReclaimed.value >= metrics.driverInitialized.value)
    assert(metrics.DFSReadSuccess.value > 0)
    assert(metrics.DFSReadFailure.value === 0)
  }

  private val retry = 2

  protected def doTest(spooky: SpookyContext): Unit = {

    doTestBeforeCache(spooky)

    doTestAfterCache(spooky)
  }

  protected def doTestAfterCache(spooky: SpookyContext): Unit = {
    Utils.retry(retry) {
      spooky.zeroMetrics()
      doMain(spooky)
      assertAfterCache(spooky)
    }
  }

  protected def doTestBeforeCache(spooky: SpookyContext): Unit = {
    Utils.retry(retry) {
      spooky.conf.pageNotExpiredSince = Some(new Date(System.currentTimeMillis()))
      spooky.zeroMetrics()
      doMain(spooky)
      assertBeforeCache(spooky)
    }
  }

  def doMain(spooky: SpookyContext): Unit

  def numFetchedPages: FetchOptimizer => Int

  def numPagesDistinct: Int = numFetchedPages(FetchOptimizers.WebCacheAware) //TODO: fix this!

  def numSessions: Int = numPagesDistinct

  def numDrivers: Int = numSessions
}

abstract class UncacheableIntegrationSuite extends IntegrationSuite {

  override protected def doTest(spooky: SpookyContext): Unit ={

    doTestBeforeCache(spooky)
  }
}