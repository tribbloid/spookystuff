package com.tribbloids.spookystuff.integration

import java.util.{Date, Properties}

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.utils.{TestHelper, Utils}
import com.tribbloids.spookystuff.{DirConf, SpookyConf, SpookyContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration

/**
 * Created by peng on 12/2/14.
 */
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

  lazy val roots = {

    val local = Seq(TestHelper.tempPath + "spooky-integration/")

    try {
      val prop = new Properties()

      prop.load(ClassLoader.getSystemResourceAsStream("rootkey.csv"))
      val AWSAccessKeyId = prop.getProperty("AWSAccessKeyId")
      val AWSSecretKey = prop.getProperty("AWSSecretKey")
      val S3Path = prop.getProperty("S3Path")

      System.setProperty("fs.s3.awsAccessKeyId", AWSAccessKeyId)
      System.setProperty("fs.s3.awsSecretAccessKey", AWSSecretKey)

      println("Test on AWS S3 with credentials provided by rootkey.csv")
      local :+ S3Path
    }
    catch {
      case e: Throwable =>
        local
    }
  }

  lazy val drivers = Seq(
    phantomJS
//    htmlUnit
  )

  lazy val optimizers = Seq(
    Narrow,
    Wide,
    Wide_RDDWebCache
  )

  import duration._

  // testing matrix
  for (root <- roots) {
    for (driver <- drivers) {
      for (optimizer <- optimizers) {
        test(s"$root, $driver, $optimizer") {
          lazy val env = new SpookyContext(
            sql,
            new SpookyConf(
              new DirConf(root = root),
              driverFactory = driver,
              defaultQueryOptimizer = optimizer,
              shareMetrics = true,
              checkpointInterval = 2,
              remoteResourceTimeout = 10.seconds
            )
          )

          doTest(env)
        }
      }
    }
  }

  //TODO: for local-cluster mode, some of these metrics may have higher than expected results because
  def assertBeforeCache(spooky: SpookyContext): Unit = {
    val metrics = spooky.metrics
    println(metrics.toJSON)

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numFetchedPages(spooky.conf.defaultQueryOptimizer))
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
    assert(pageFetched === numFetchedPages(spooky.conf.defaultQueryOptimizer))
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

  protected def doTest(spooky: SpookyContext): Unit ={

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

  def numFetchedPages: QueryOptimizer => Int

  def numPagesDistinct: Int = numFetchedPages(Wide_RDDWebCache)

  def numSessions: Int = numPagesDistinct

  def numDrivers: Int = numSessions
}

abstract class UncacheableIntegrationSuite extends IntegrationSuite {

  override protected def doTest(spooky: SpookyContext): Unit ={

    doTestBeforeCache(spooky)
  }
}