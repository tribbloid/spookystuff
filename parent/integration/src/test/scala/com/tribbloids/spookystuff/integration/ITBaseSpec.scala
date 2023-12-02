package com.tribbloids.spookystuff.integration

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.conf.{DriverFactory, SpookyConf}
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.metrics.SpookyMetrics
import com.tribbloids.spookystuff.testutils.{LocalURIDocsFixture, SpookyBaseSpec, TestHelper}
import com.tribbloids.spookystuff.utils.{CommonConst, CommonUtils}
import com.tribbloids.spookystuff.web.conf.{Web, WebDriverFactory}
import com.tribbloids.spookystuff.web.session.CleanWebDriver
import org.slf4j.LoggerFactory

import java.util.Date
import scala.concurrent.duration
import scala.util.Random

abstract class ITBaseSpec extends SpookyBaseSpec with LocalURIDocsFixture {

  val phantomJS: WebDriverFactory.PhantomJS = WebDriverFactory.PhantomJS()
  val htmlUnit: WebDriverFactory.HtmlUnit = WebDriverFactory.HtmlUnit()

  lazy val roots: Seq[String] = {

    val local = Seq(CommonUtils.\\\(CommonConst.USER_TEMP_DIR, "spooky-integration"))

    local ++ TestHelper.S3Path
  }

  lazy val driverFactories: Seq[DriverFactory[CleanWebDriver]] = Seq(
    phantomJS,
    phantomJS.taskLocal
    //    htmlUnit
  )

  lazy val genPartitioners: Seq[GenPartitionerLike[Trace, Any]] = Seq(
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

          _spooky = SpookyContext(sql)
          _spooky.setConf(
            SpookyConf(
              defaultGenPartitioner = gp,
              epochSize = 1 + Random.nextInt(4),
              shareMetrics = true,
              remoteResourceTimeout = 10.seconds
            ),
            Web.Conf(
              webDriverFactory = driver
            )
          )

          doTest()
        }
      }
    }
  }

  // TODO: for local-cluster mode, some of these metrics may have higher than expected results because.
  def assertBeforeCache(): Unit = {
    val metrics: SpookyMetrics = spooky.spookyMetrics
    val metricsJSON: String = metrics.toTreeIR.toJSON()
    LoggerFactory.getLogger(this.getClass).info(metricsJSON)

    val pagesFetched = metrics.pagesFetched.value
    remotePagesFetched = metrics.pagesFetchedFromRemote.value
    assert(pagesFetched >= numPages)
    assert(remoteFetchSuboptimality contains remotePagesFetched - numPages)
    assert(metrics.pagesFetchedFromCache.value === pagesFetched - remotePagesFetched)
    assert(metrics.sessionInitialized.value === numSessions)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.Drivers.dispatchedTotalCount === numWebDrivers)
    assert(metrics.Drivers.releasedTotalCount >= numWebDrivers)
    assert(pagesFetched <= pageFetchedCap)
  }

  def assertAfterCache(): Unit = {
    val metrics: SpookyMetrics = spooky.spookyMetrics
    val metricsJSON: String = metrics.toTreeIR.toJSON()
    LoggerFactory.getLogger(this.getClass).info(metricsJSON)

    val pagesFetched = metrics.pagesFetched.value
    assert(pagesFetched >= numPages)
    assert(metrics.pagesFetchedFromRemote.value === 0)
    assert(metrics.pagesFetchedFromCache.value === pagesFetched)
    assert(metrics.sessionInitialized.value === 0)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.Drivers.dispatchedTotalCount === 0)
    assert(metrics.Drivers.releasedTotalCount >= 0)
    //    assert(metrics.DFSReadSuccess.value > 0) //TODO: enable this after more detailed control over 2 caches.
    assert(metrics.DFSReadFailure.value === 0)
//    assert(pagesFetched <= pageFetchedCap) // cache read is too fast, its hard to optimize
  }

  private val retry = 3

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestSiteServer.server.start()
  }

  protected def doTest(): Unit = {

    doTestBeforeCache()

    doTestAfterCache()
  }

  protected def doTestBeforeCache(): Unit = {
    CommonUtils.retry(retry) {
      spooky.spookyConf.IgnoreCachedDocsBefore = Some(new Date(System.currentTimeMillis()))
      spooky.Plugins.resetAll()
      doMain()
      assertBeforeCache()
    }
  }
  protected def doTestAfterCache(): Unit = {
    CommonUtils.retry(retry) {
      spooky.Plugins.resetAll()
      doMain()
      assertAfterCache()
    }
  }

  def doMain(): Unit

  def numPages: Long
  var remotePagesFetched: Long = _

  def numSessions: Long = remotePagesFetched
  final def numWebDrivers: Long = {
    if (driverFactories.flatMap(Option(_)).isEmpty) 0
    else numSessions
  }

  def pageFetchedCap: Long = numPages * 2

  def remoteFetchSuboptimality: Range = 0 to 0
}
