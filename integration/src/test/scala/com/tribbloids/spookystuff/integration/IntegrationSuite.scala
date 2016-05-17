package com.tribbloids.spookystuff.integration

import java.util.Date

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.tests.TestHelper
import com.tribbloids.spookystuff.utils.Utils
import com.tribbloids.spookystuff.{DirConf, SpookyConf, SpookyContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration
import scala.language.implicitConversions

abstract class IntegrationSuite extends FunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _
  @transient var sql: SQLContext = _
  var spooky: SpookyContext = _

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
          spooky = new SpookyContext(
            sql,
            new SpookyConf(
              new DirConf(root = root),
              driverFactory = driver,
              defaultFetchOptimizer = optimizer,
              shareMetrics = true,
              remoteResourceTimeout = 10.seconds
            )
          )

          doTest()
        }
      }
    }
  }

  //TODO: for local-cluster mode, some of these metrics may have higher than expected results because.
  def assertBeforeCache(): Unit = {
    val metrics = spooky.metrics
    println(metrics.toJSON)

    val pagesFetched = metrics.pagesFetched.value
    numPages_actual = metrics.pagesFetchedFromRemote.value
    assert(pagesFetched >= numPages)
    assert(error contains numPages_actual - numPages)
    assert(metrics.pagesFetchedFromCache.value === pagesFetched - numPages_actual)
    assert(metrics.sessionInitialized.value === numSessions)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === numDrivers)
    assert(metrics.driverReclaimed.value >= metrics.driverInitialized.value)
  }

  def assertAfterCache(): Unit = {
    val metrics = spooky.metrics
    println(metrics.toJSON)

    val pagesFetched = metrics.pagesFetched.value
    assert(pagesFetched >= numPages)
    assert(metrics.pagesFetchedFromRemote.value === 0)
    assert(metrics.pagesFetchedFromCache.value === pagesFetched)
    assert(metrics.sessionInitialized.value === 0)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === 0)
    assert(metrics.driverReclaimed.value >= metrics.driverInitialized.value)
    assert(metrics.DFSReadSuccess.value > 0)
    assert(metrics.DFSReadFailure.value === 0)
  }

  private val retry = 2

  protected def doTest(): Unit = {

    doTestBeforeCache()

    doTestAfterCache()
  }

  protected def doTestAfterCache(): Unit = {
    Utils.retry(retry) {
      spooky.zeroMetrics()
      doMain()
      assertAfterCache()
    }
  }

  protected def doTestBeforeCache(): Unit = {
    Utils.retry(retry) {
      spooky.conf.pageNotExpiredSince = Some(new Date(System.currentTimeMillis()))
      spooky.zeroMetrics()
      doMain()
      assertBeforeCache()
    }
  }

  def doMain(): Unit

  def numPages: Int
  var numPages_actual: Int = _
  def numSessions: Int = numPages_actual
  def numDrivers: Int = numSessions

  def error: Range = 0 to 0
}

abstract class UncacheableIntegrationSuite extends IntegrationSuite {

  override protected def doTest(): Unit ={

    doTestBeforeCache()
  }
}