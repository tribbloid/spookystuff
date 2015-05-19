package org.tribbloid.spookystuff.integration

import java.util.{Date, Properties}

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
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "org.tribbloid.spookystuff.SpookyRegistrator")
//      .set("spark.kryoserializer.buffer.max.mb", "512")
//      .set("spark.kryo.registrationRequired", "true")

    sc = new SparkContext(conf)

    try {
      val prop = new Properties()

      prop.load(ClassLoader.getSystemResourceAsStream("rootkey.csv"))
      val AWSAccessKeyId = prop.getProperty("AWSAccessKeyId")
      val AWSSecretKey = prop.getProperty("AWSSecretKey")

      sc.hadoopConfiguration
        .set("fs.s3n.awsAccessKeyId", AWSAccessKeyId)
      sc.hadoopConfiguration
        .set("fs.s3n.awsSecretAccessKey", AWSSecretKey)
    }
    catch {
      case e: Throwable => println("rootkey.csv not provided")
    }

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
    //    "s3a://spooky-integration/"
  )

  lazy val drivers = Seq(
    DriverFactories.PhantomJS(),
    DriverFactories.HtmlUnit()
  )

  lazy val optimizers = Seq(
    Narrow,
    Wide,
    Wide_WebCachedRDD
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
            checkpointInterval = 2
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

    val numPages = this.numPages(spooky.conf.defaultQueryOptimizer)

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numPages)
    assert(metrics.pagesFetchedFromWeb.value === numPagesDistinct)
    assert(metrics.pagesFetchedFromCache.value === numPages - numPagesDistinct)
    assert(metrics.sessionInitialized.value === numSessions)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === numDrivers)
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

  private val retry = 2

  private def doTest(spooky: SpookyContext): Unit ={

    Utils.retry(retry) {
      spooky.conf.pageNotExpiredSince = Some(new Date(System.currentTimeMillis()))
      spooky.zeroMetrics()
      doMain(spooky)
      assertBeforeCache(spooky)
    }

    Utils.retry(retry) {
      spooky.zeroMetrics()
      doMain(spooky)
      assertAfterCache(spooky)
    }
  }

  def doMain(spooky: SpookyContext): Unit

  def numPages: QueryOptimizer => Int

  def numPagesDistinct: Int = numPages(Wide_WebCachedRDD)

  def numSessions: Int = numPagesDistinct

  def numDrivers: Int = numSessions
}