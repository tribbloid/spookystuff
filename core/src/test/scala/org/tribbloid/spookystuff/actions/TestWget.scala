package org.tribbloid.spookystuff.actions

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.factory.{TorProxySetting, PageBuilder}

/**
 * Created by peng on 11/6/14.
 */
class TestWget extends FunSuite {

  lazy val conf: SparkConf = new SparkConf().setAppName("dummy").setMaster("local")
  lazy val sc: SparkContext = new SparkContext(conf)
  lazy val sql: SQLContext = new SQLContext(sc)
  lazy implicit val spooky: SpookyContext = new SpookyContext(sql)
  spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")
  spooky.autoSave = false
  spooky.autoCache = false
  spooky.autoRestore = false

  val noProxyIP = {
    val results = PageBuilder.resolve(
      Wget("http://www.whatsmyip.org/") :: Nil,
      //      Wget("https://www.google.hk") :: Nil,
      dead = false
    )(spooky)

    results(0).text1("h1 span")
  }

  //TODO: find a test site for https!
  test("use TOR socks5 proxy for https") {

    spooky.proxy = TorProxySetting()

    val results = PageBuilder.resolve(
      Wget("https://www.whatsmyip.org/") :: Nil,
//      Wget("https://www.google.hk") :: Nil,
      dead = false
    )(spooky)

    val newIP = results(0).text1("h1 span")

    assert(results(0).text1("title") === "What's My IP Address? Networking Tools & More")
    assert(newIP !== null)
    assert(newIP !== "")
    assert(newIP !== noProxyIP)
  }

  test("use TOR socks5 proxy for http") {

    spooky.proxy = TorProxySetting()

    val results = PageBuilder.resolve(
      Wget("http://www.whatsmyip.org/") :: Nil,
      //      Wget("https://www.google.hk") :: Nil,
      dead = false
    )(spooky)

    val newIP = results(0).text1("h1 span")

    assert(results(0).text1("title") === "What's My IP Address? Networking Tools & More")
    assert(newIP !== null)
    assert(newIP !== "")
    assert(newIP !== noProxyIP)
  }
}
