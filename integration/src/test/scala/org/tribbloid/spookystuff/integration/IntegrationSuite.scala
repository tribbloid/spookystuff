package org.tribbloid.spookystuff.integration

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.tribbloid.spookystuff.SpookyConf.Dirs
import org.tribbloid.spookystuff.dsl._
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

  lazy val roots = Seq(
    "file://"+System.getProperty("user.home")+"/spooky-integration/"
//    "s3n://spooky-integration/"
  )

  lazy val drivers = Seq(
    PhantomJSDriverFactory(),
    HtmlUnitDriverFactory()
  )

  lazy val optimizers = Seq(
    Narrow,
    WideLookup
  )

  for (root <- roots) {
    for (driver <- drivers) {
      for (optimizer <- optimizers) {
        lazy val env = new SpookyContext(
          sql,
          new SpookyConf(
            new Dirs(root = root),
            driverFactory = driver,
            defaultQueryOptimizer = optimizer,
            sharedMetrics = true,
            checkpointInterval = 2,
            batchSize = 2
          )
        )

        test(s"$root, $driver, $optimizer") {
          doTest(env)
        }
      }
    }
  }

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

  private val retry = 1

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

  def doMain(spooky: SpookyContext): Unit

  def numPages: QueryOptimizer => Int

  def numSessions: QueryOptimizer => Int = numPages

  def numDrivers: QueryOptimizer => Int = numSessions
}