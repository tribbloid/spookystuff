package com.tribbloids.spookystuff.integration

import java.util.Date

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.testutils.{RemoteDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration
import scala.util.Random

abstract class IntegrationFixture extends SpookyEnvFixture with BeforeAndAfterAll with RemoteDocsFixture {

  val phantomJS = DriverFactories.PhantomJS()
  val htmlUnit = DriverFactories.HtmlUnit()

  lazy val roots: Seq[String] = {

    val local = Seq(SpookyUtils.\\\(TestHelper.TEMP_PATH, "spooky-integration"))

    local ++ TestHelper.S3Path
  }

  lazy val driverFactories = Seq(
    phantomJS,
    phantomJS.taskLocal
    //    htmlUnit
  )

  lazy val genPartitioners = Seq(
    GenPartitioners.Narrow,
    GenPartitioners.Wide(),
    GenPartitioners.DocCacheAware()
  )

  import duration._

  // testing matrix
  for (root <- roots) {
    for (driver <- driverFactories) {
      for (gp <- genPartitioners) {
        it(s"$gp/$driver/$root") {
          _spooky = new SpookyContext(
            sql,
            new SpookyConf(
              submodules = envComponents,
              webDriverFactory = driver,
              defaultGenPartitioner = gp,
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
    val metrics: SpookyMetrics = spooky.metrics
    val metricsJSON: String = metrics.toJSON() //TODO: this will trigger a compiler bug in scala 2.10.6, need to fix it!
    println(metricsJSON)

    val pagesFetched = metrics.pagesFetched.value
    remotePagesFetched = metrics.pagesFetchedFromRemote.value
    assert(pagesFetched >= numPages)
    assert(error contains remotePagesFetched - numPages)
    assert(metrics.pagesFetchedFromCache.value === pagesFetched - remotePagesFetched)
    assert(metrics.sessionInitialized.value === numSessions)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.webDriverDispatched.value === numDrivers)
    assert(metrics.webDriverReleased.value >= metrics.webDriverDispatched.value)
    assert(pagesFetched <= pageFetchedCap)
  }

  def assertAfterCache(): Unit = {
    val metrics: SpookyMetrics = spooky.metrics
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
//    assert(pagesFetched <= pageFetchedCap) // cache read is too fast, its hard to optimize
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
  var remotePagesFetched: Int = _
  def numSessions: Int = remotePagesFetched
  final def numDrivers: Int = {
    if (driverFactories.flatMap(Option(_)).isEmpty) 0
    else numSessions
  }

  def pageFetchedCap: Int = numPages * 2

  def error: Range = 0 to 0
}

abstract class UncacheableIntegrationFixture extends IntegrationFixture {

  override protected def doTest(): Unit ={

    doTestBeforeCache()
  }
}