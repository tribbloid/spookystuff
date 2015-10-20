package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.pages.Unstructured
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkEnv, SparkConf, SparkContext}
import org.scalatest.{Retries, BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import com.tribbloids.spookystuff.dsl.{DriverFactories, DriverFactory}
import com.tribbloids.spookystuff.utils.{TestHelper, Utils}

/**
 * Created by peng on 11/30/14.
 */
abstract class SpookyEnvSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll with Retries {

  var sc: SparkContext = _
  var sql: SQLContext = _
  var spooky: SpookyContext = _

  lazy val driverFactory: DriverFactory = DriverFactories.PhantomJS(loadImages = true)

  override def withFixture(test: NoArgTest) = {
    if (isRetryable(test))
      Utils.retry(4) { super.withFixture(test) }
    else
      super.withFixture(test)
  }

  override def beforeAll() {
    val conf: SparkConf = TestHelper.testSparkConf.setAppName("unit")

    sc = new SparkContext(conf)

    sql = new SQLContext(sc)

    val sConf = new SpookyConf(
      driverFactory = driverFactory
    )

    spooky = new SpookyContext(sql, sConf)

    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }

    TestHelper.clearTempDir()
    super.afterAll()
  }

  before{
    setUp()
  }

  def setUp(): Unit = {

    spooky.conf = new SpookyConf(
      autoSave = true,
      cacheWrite = false,
      cacheRead = false,
      dirs = new DirConf(
        root = TestHelper.tempPath + "spooky-unit/"
      )
    )
  }
}