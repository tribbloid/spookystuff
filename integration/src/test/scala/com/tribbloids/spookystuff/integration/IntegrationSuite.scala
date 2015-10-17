package com.tribbloids.spookystuff.integration

import java.util.{Date, Properties}

import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.utils.Utils
import com.tribbloids.spookystuff.{DirConf, SpookyConf, SpookyContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration

/**
 * Created by peng on 12/2/14.
 */
abstract class IntegrationSuite extends FunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _
  @transient var sql: SQLContext = _

  override def beforeAll() {
    val conf: SparkConf = new SparkConf().setAppName("integration")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.tribbloids.spookystuff.SpookyRegistrator")
      .set("spark.kryoserializer.buffer.max", "512m")

    val sparkHome = System.getenv("SPARK_HOME")
    if (sparkHome == null) {
      println("initialization Spark Context in local mode")
      conf.setMaster(s"local[${Runtime.getRuntime.availableProcessors()},4]")
    }
    else {
      println("initialization Spark Context in local-cluster simulation mode")
      conf
        .setMaster("local-cluster[4,4,512]")
        .setSparkHome(sparkHome)
        .set("spark.driver.extraClassPath", sys.props("java.class.path"))
        .set("spark.executor.extraClassPath", sys.props("java.class.path"))
    }

    sc = new SparkContext(conf)
    sql = new SQLContext(sc)

    //TODO: why do I have to do this?
    Option(System.getProperty("fs.s3n.awsAccessKeyId")).foreach {
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", _)
    }
    Option(System.getProperty("fs.s3n.awsSecretAccessKey")).foreach {
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", _)
    }

    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

  lazy val roots = {

    val local = Seq("file://"+System.getProperty("user.dir")+"/temp/spooky-integration/")

    try {
      val prop = new Properties()

      prop.load(ClassLoader.getSystemResourceAsStream("rootkey.csv"))
      val AWSAccessKeyId = prop.getProperty("AWSAccessKeyId")
      val AWSSecretKey = prop.getProperty("AWSSecretKey")

      System.setProperty("fs.s3n.awsAccessKeyId", AWSAccessKeyId)
      System.setProperty("fs.s3n.awsSecretAccessKey", AWSSecretKey)

      local :+ "s3n://spooky-integration/"
    }
    catch {
      case e: Throwable =>
        println("rootkey.csv not provided")
        local
    }
  }

  lazy val drivers = Seq(
    DriverFactories.PhantomJS(),
    DriverFactories.HtmlUnit()
  )

  lazy val optimizers = Seq(
    Narrow,
    Wide,
    Wide_RDDWebCache
  )

  import duration._

  for (root <- roots) {
    for (driver <- drivers) {
      for (optimizer <- optimizers) {
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

        test(s"$root, $driver, $optimizer") {
          doTest(env)
        }
      }
    }
  }

  def assertBeforeCache(spooky: SpookyContext): Unit = {
    val metrics = spooky.metrics

    val numPages = this.numFetchedPages(spooky.conf.defaultQueryOptimizer)

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numPages)
    assert(metrics.pagesFetchedFromWeb.value === numPagesDistinct)
    assert(metrics.pagesFetchedFromCache.value === numPages - numPagesDistinct)
    assert(metrics.sessionInitialized.value === numSessions)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === numDrivers)
    assert(metrics.driverReclaimed.value >= metrics.driverInitialized.value)
  }

  def assertAfterCache(spooky: SpookyContext): Unit = {
    val metrics = spooky.metrics

    val pageFetched = metrics.pagesFetched.value
    assert(pageFetched === numFetchedPages(spooky.conf.defaultQueryOptimizer))
    assert(metrics.pagesFetchedFromWeb.value === 0)
    assert(metrics.pagesFetchedFromCache.value === pageFetched)
    assert(metrics.sessionInitialized.value === 0)
    assert(metrics.sessionReclaimed.value >= metrics.sessionInitialized.value)
    assert(metrics.driverInitialized.value === 0)
    assert(metrics.driverReclaimed.value >= metrics.driverInitialized.value)
    assert(metrics.DFSReadSuccess.value > 0)
    assert(metrics.DFSReadFailure.value === 0)
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

  def numFetchedPages: QueryOptimizer => Int

  def numPagesDistinct: Int = numFetchedPages(Wide_RDDWebCache)

  def numSessions: Int = numPagesDistinct

  def numDrivers: Int = numSessions
}

abstract class UncacheableIntegrationSuite extends IntegrationSuite {

  override def assertAfterCache(spooky: SpookyContext) = this.assertBeforeCache(spooky)
}