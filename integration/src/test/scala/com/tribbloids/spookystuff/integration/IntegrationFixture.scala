package com.tribbloids.spookystuff.integration

import java.util.Date

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.testutils.{RemoteDocsFixture, TestHelper}
import com.tribbloids.spookystuff.utils.{CommonConst, CommonUtils}
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration
import scala.util.Random

abstract class IntegrationFixture extends SpookyEnvFixture with BeforeAndAfterAll with RemoteDocsFixture {

  val phantomJS = DriverFactories.PhantomJS()
  val htmlUnit = DriverFactories.HtmlUnit()

  lazy val roots: Seq[String] = {

    val local = Seq(CommonUtils.\\\(CommonConst.TEMP_DIR, "spooky-integration"))

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
          val submodules = this.submodules.transform {
            case _: SpookyConf =>
              new SpookyConf(
                webDriverFactory = driver,
                defaultGenPartitioner = gp,
                epochSize = 1 + Random.nextInt(4),
                shareMetrics = true,
                remoteResourceTimeout = 10.seconds
              )
            case v @ _ => v
          }

          _spooky = SpookyContext(
            sql,
            submodules
          )

          doTest()
        }
      }
    }
  }

  //TODO: for local-cluster mode, some of these metrics may have higher than expected results because.
  def assertBeforeCache(): Unit = {
    val metrics: SpookyMetrics = spooky.spookyMetrics
    val metricsJSON: String = metrics.toJSON() //TODO: this will trigger a compiler bug in scala 2.10.6, need to fix it!
    println(metricsJSON)

    val pagesFetched = metrics.pagesFetched.value
    remotePagesFetched = metrics.pagesFetchedFromRemote.value
    assert(pagesFetched >= numPages)
    assert(remoteFetchSuboptimality contains remotePagesFetched - numPages)
    assert(metrics.pagesFetchedFromCache.value === pagesFetched - remotePagesFetched)
    assert(metrics.sessionInitialized.value === numSessions)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.webDriverDispatched.value === numDrivers)
    assert(metrics.webDriverReleased.value >= metrics.webDriverDispatched.value)
    assert(pagesFetched <= pageFetchedCap)
  }

  def assertAfterCache(): Unit = {
    val metrics: SpookyMetrics = spooky.spookyMetrics
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

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestSiteServer.server.start()
  }

  protected def doTest(): Unit = {

    beforeAll()

    doTestBeforeCache()

    doTestAfterCache()
  }

  protected def doTestAfterCache(): Unit = {
    CommonUtils.retry(retry) {
      spooky.zeroMetrics()
      doMain()
      assertAfterCache()
    }
  }

  protected def doTestBeforeCache(): Unit = {
    CommonUtils.retry(retry) {
      spooky.spookyConf.IgnoreCachedDocsBefore = Some(new Date(System.currentTimeMillis()))
      spooky.zeroMetrics()
      doMain()
      assertBeforeCache()
    }
  }

  def doMain(): Unit

  def numPages: Long
  var remotePagesFetched: Long = _
  def numSessions: Long = remotePagesFetched
  final def numDrivers: Long = {
    if (driverFactories.flatMap(Option(_)).isEmpty) 0
    else numSessions
  }

  def pageFetchedCap: Long = numPages * 2

  def remoteFetchSuboptimality: Range = 0 to 0
}

abstract class UncacheableIntegrationFixture extends IntegrationFixture {

  override protected def doTest(): Unit = {

    doTestBeforeCache()
  }
}
