package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.dsl.{DriverFactories, DriverFactory}
import com.tribbloids.spookystuff.tests.{RemoteDocsMixin, TestHelper}
import com.tribbloids.spookystuff.utils.Utils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Retries}

/**
 * Created by peng on 11/30/14.
 */
abstract class SpookyEnvSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll with Retries with RemoteDocsMixin {

  def sc: SparkContext = TestHelper.TestSpark
  def sql: SQLContext = TestHelper.TestSQL
  var spooky: SpookyContext = _

  lazy val driverFactory: DriverFactory = DriverFactories.PhantomJS(loadImages = true)

  override def withFixture(test: NoArgTest) = {
    if (isRetryable(test))
      Utils.retry(4) { super.withFixture(test) }
    else
      super.withFixture(test)
  }

  override def beforeAll() {

    val spookyConf = new SpookyConf(
      driverFactory = driverFactory
    )

    spooky = new SpookyContext(sql, spookyConf)

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