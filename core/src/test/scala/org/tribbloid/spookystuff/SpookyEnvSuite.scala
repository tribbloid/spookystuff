package org.tribbloid.spookystuff

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Retries, BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.tribbloid.spookystuff.dsl.{DriverFactory, PhantomJSDriverFactory}

/**
 * Created by peng on 11/30/14.
 */
abstract class SpookyEnvSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll with Retries {

  var sc: SparkContext = _
  var sql: SQLContext = _
  var spooky: SpookyContext = _

  lazy val driverFactory: DriverFactory = PhantomJSDriverFactory(loadImages = true)

  override def withFixture(test: NoArgTest) = {
    if (isRetryable(test))
      withRetry { super.withFixture(test) }
    else
      super.withFixture(test)
  }

  override def beforeAll() {
    val conf: SparkConf = new SparkConf().setAppName("integration")
      .setMaster("local[*]")

    sc = new SparkContext(conf)

    val sql: SQLContext = new SQLContext(sc)

    val sConf = new SpookyConf(
      driverFactory = driverFactory,
      autoSave = false,
      cacheWrite = false,
      cacheRead = false
    )

    sConf.dirs.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")

    spooky = new SpookyContext(sql, sConf)

    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

  before{
    spooky.conf.autoSave = false
    spooky.conf.cacheWrite = false
    spooky.conf.cacheRead = false

    spooky.conf.dirs.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")
  }
}
