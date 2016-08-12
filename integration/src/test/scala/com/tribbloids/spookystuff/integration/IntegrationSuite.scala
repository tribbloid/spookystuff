package com.tribbloids.spookystuff.integration

import java.util.Date

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.tests.{RemoteDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.{DirConf, SpookyConf, SpookyContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration
import scala.language.implicitConversions
import scala.util.Random

abstract class IntegrationSuite extends FunSuite with BeforeAndAfterAll with RemoteDocsFixture {

  def sc: SparkContext = TestHelper.TestSpark
  def sql: SQLContext = TestHelper.TestSQL

  var spooky: SpookyContext = _

  val phantomJS = DriverFactories.PhantomJS()
  val htmlUnit = DriverFactories.HtmlUnit()

  override def beforeAll() {
    TestHelper.TestSparkConf.setAppName("integration")

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

  lazy val driverFactories = Seq(
    phantomJS,
    phantomJS.pooled
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
    for (driver <- driverFactories) {
      for (optimizer <- optimizers) {
        test(s"$optimizer/$driver/$root") {
          spooky = new SpookyContext(
            sql,
            new SpookyConf(
              new DirConf(root = root),
              webDriverFactory = driver,
              defaultFetchOptimizer = optimizer,
              epochSize = 1 + Random.nextInt(4),
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
    assert(metrics.driverGet.value === numDrivers)
    assert(metrics.driverReleased.value >= metrics.driverGet.value)
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
    assert(metrics.driverGet.value === 0)
    assert(metrics.driverReleased.value >= metrics.driverGet.value)
    //    assert(metrics.DFSReadSuccess.value > 0) //TODO: enable this after more detailed control over 2 caches.
    assert(metrics.DFSReadFailure.value === 0)
  }

  private val retry = 3

  protected def doTest(): Unit = {

    doTestBeforeCache()

    doTestAfterCache()
  }

  protected def doTestAfterCache(): Unit = {
    SpookyUtils.retry(retry) {
      spooky.zeroMetrics()
      doMain()
      assertAfterCache()
    }
  }

  protected def doTestBeforeCache(): Unit = {
    SpookyUtils.retry(retry) {
      spooky.conf.IgnoreDocsCreatedBefore = Some(new Date(System.currentTimeMillis()))
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