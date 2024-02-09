package com.tribbloids.spookystuff.integration

import ai.acyclic.prover.commons.spark.Envs
import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.conf.{Core, DriverFactory, SpookyConf}
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.metrics.SpookyMetrics
import com.tribbloids.spookystuff.row.LocalityGroup
import com.tribbloids.spookystuff.testutils.{FileURIDocsFixture, SpookyBaseSpec, TestHelper}
import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.web.conf.{Web, WebDriverFactory}
import com.tribbloids.spookystuff.web.agent.CleanWebDriver
import org.slf4j.LoggerFactory

import java.util.Date
import scala.concurrent.duration
import scala.util.Random

abstract class ITBaseSpec extends SpookyBaseSpec with FileURIDocsFixture {

  val phantomJS: WebDriverFactory.PhantomJS = WebDriverFactory.PhantomJS()
  val htmlUnit: WebDriverFactory.HtmlUnit = WebDriverFactory.HtmlUnit()

  lazy val roots: Seq[String] = {

    val local: Seq[String] = Seq(Envs.USER_TEMP_DIR :\ "spooky-integration")

    local ++ TestHelper.S3Path
  }

  lazy val webDriverFactories: Seq[DriverFactory[CleanWebDriver]] = Seq(
    phantomJS,
    phantomJS.taskLocal
    //    htmlUnit
  )

  lazy val genPartitioners: Seq[GenPartitionerLike[LocalityGroup, Any]] = Seq(
    GenPartitioners.Narrow,
    GenPartitioners.Wide(),
    GenPartitioners.DocCacheAware()
  )

  import duration._

  // testing matrix
  for (case (root, rootI) <- roots.zipWithIndex) {
    for (driver <- webDriverFactories) {
      for (gp <- genPartitioners) {
        it(s"$gp - $driver - $rootI") {

          _ctxOverride = SpookyContext(sql)
          _ctxOverride.setConf(
            SpookyConf(
              localityPartitioner = gp,
              exploreEpochSize = 1 + Random.nextInt(4),
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
    assert(maxRedundantFetch contains remotePagesFetched - numPages)
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
      spooky(Core).confUpdate(
        _.copy(IgnoreCachedDocsBefore = Some(new Date(System.currentTimeMillis())))
      )

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
    if (webDriverFactories.flatMap(Option(_)).isEmpty) 0
    else numSessions
  }

  def pageFetchedCap: Long = numPages * 2

  def maxRedundantFetch: Range = 0 to 0
}
