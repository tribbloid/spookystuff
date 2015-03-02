package org.tribbloid.spookystuff

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.tribbloid.spookystuff.dsl.PhantomJSDriverFactory

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

    val sConf = new SpookyConf(
      driverFactory = PhantomJSDriverFactory(loadImages = true),
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
