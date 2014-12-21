package org.tribbloid.spookystuff

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.tribbloid.spookystuff.dsl.NaiveDriverFactory

/**
 * Created by peng on 11/30/14.
 */
abstract class SpookyEnvSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  var sc: SparkContext = _
  var sql: SQLContext = _
  var spooky: SpookyContext = _

  override def beforeAll() {
    val conf: SparkConf = new SparkConf().setAppName("integration")
      .setMaster("local[*]")

    sc = new SparkContext(conf)

    val sql: SQLContext = new SQLContext(sc)

    spooky = new SpookyContext(
      sql,
      driverFactory = NaiveDriverFactory(loadImages = true)
    )

    spooky.autoSave = false
    spooky.cacheWrite = false
    spooky.cacheRead = false

    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")

    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

  before{
    spooky.autoSave = false
    spooky.cacheWrite = false
    spooky.cacheRead = false

    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")
  }
}
