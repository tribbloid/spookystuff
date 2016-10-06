package com.tribbloids.spookystuff.integration

import java.util.Date

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.testutils.{RemoteDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration
import scala.util.Random

abstract class IntegrationFixture extends SpookyEnvFixture with BeforeAndAfterAll with RemoteDocsFixture {

  val phantomJS = DriverFactories.PhantomJS()
  val htmlUnit = DriverFactories.HtmlUnit()

  lazy val roots: Seq[String] = {

    val local = Seq(TestHelper.TEMP_PATH + "spooky-integration/")

    local ++ TestHelper.S3Path
  }

  lazy val driverFactories = Seq(
    phantomJS,
    phantomJS.pooling
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
          _spooky = new SpookyContext(
            sql,
            new SpookyConf(
              components = Map(
                "dirs" -> new DirConf(
                  root = root
                )
              ),
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
    val metrics: Metrics = spooky.metrics
    val metricsJSON: String = metrics.toJSON() //TODO: this will trigger a compiler bug in scala 2.10.6, need to fix it!
    println(metricsJSON)

    val pagesFetched = metrics.pagesFetched.value
    numPages_distinct = metrics.pagesFetchedFromRemote.value
    assert(pagesFetched >= numPages)
    assert(error contains numPages_distinct - numPages)
    assert(metrics.pagesFetchedFromCache.value === pagesFetched - numPages_distinct)
    assert(metrics.sessionInitialized.value === numSessions)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.webDriverDispatched.value === numDrivers)
    assert(metrics.webDriverReleased.value >= metrics.webDriverDispatched.value)
  }

  def assertAfterCache(): Unit = {
    val metrics: Metrics = spooky.metrics
    val metricsJSON: String = metrics.toJSON()
    println(metricsJSON)

    val pagesFetched = metrics.pagesFetched.value
    assert(pagesFetched >= numPages)
    assert(metrics.pagesFetchedFromRemote.value === 0)
    assert(metrics.pagesFetchedFromCache.value === pagesFetched)
    assert(metrics.sessionInitialized.value === 0)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.webDriverDispatched.value === 0)
    assert(metrics.webDriverReleased.value >= metrics.webDriverDispatched.value)
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
      spooky.conf.IgnoreCachedDocsBefore = Some(new Date(System.currentTimeMillis()))
      spooky.zeroMetrics()
      doMain()
      assertBeforeCache()
    }
  }

  def doMain(): Unit

  def numPages: Int
  var numPages_distinct: Int = _
  def numSessions: Int = numPages_distinct
  def numDrivers: Int = numSessions

  def error: Range = 0 to 0
}

abstract class UncacheableIntegrationFixture extends IntegrationFixture {

  override protected def doTest(): Unit ={

    doTestBeforeCache()
  }
}