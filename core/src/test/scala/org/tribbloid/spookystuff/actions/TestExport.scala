package org.tribbloid.spookystuff.actions

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.factory.driver.TorProxyFactory

/**
 * Created by peng on 11/6/14.
 */
class TestExport extends FunSuite {

  lazy val conf: SparkConf = new SparkConf().setAppName("dummy").setMaster("local")
  lazy val sc: SparkContext = new SparkContext(conf)
  lazy val sql: SQLContext = new SQLContext(sc)
  lazy implicit val spooky: SpookyContext = new SpookyContext(sql)
  spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")
  spooky.autoSave = false
  spooky.autoCache = false
  spooky.autoRestore = false
  lazy val noProxyIP = {
      spooky.proxy = () => null

      val results = Trace(
        Wget("http://www.whatsmyuseragent.com/") :: Nil
      ).resolvePlain(spooky)

      results(0).text1("h3.info")
    }

  override def finalize(){
    sc.stop()
  }

  test("use TOR socks5 proxy for http wget") {

    val newIP = {
      spooky.proxy = TorProxyFactory

      val results = Trace(
        Wget("http://www.whatsmyuseragent.com/") :: Nil
      ).resolvePlain(spooky)

      results(0).text1("h3.info")
    }

    assert(newIP !== null)
    assert(newIP !== "")
    assert(newIP !== noProxyIP)
  }

  //TODO: find a test site for https!
  test("use TOR socks5 proxy for https wget") {

    val newIP = {
      spooky.proxy = TorProxyFactory

      val results = Trace(
        Wget("https://www.astrill.com/what-is-my-ip-address.php") :: Nil
      ).resolvePlain(spooky)

      results(0).text1("h1")
    }

    assert(newIP !== null)
    assert(newIP !== "")
    assert(newIP !== noProxyIP)
  }

  test("revert proxy setting for http wget") {

    val newIP = {
      spooky.proxy = TorProxyFactory

      val results = Trace(
        Wget("http://www.whatsmyuseragent.com/") :: Nil
      ).resolvePlain(spooky)

      results(0).text1("h3.info")
    }

    val noProxyIP2 = {
      spooky.proxy = () => null

      val results = Trace(
        Wget("http://www.whatsmyuseragent.com/") :: Nil
      ).resolvePlain(spooky)

      results(0).text1("h3.info")
    }

    assert(newIP !== noProxyIP2)
  }

  test("revert proxy setting for https wget") {

    val newIP = {
      spooky.proxy = TorProxyFactory

      val results = Trace(
        Wget("https://www.astrill.com/what-is-my-ip-address.php") :: Nil
      ).resolvePlain(spooky)

      results(0).text1("h1")
    }

    val noProxyIP2 = {
      spooky.proxy = () => null

      val results = Trace(
        Wget("https://www.astrill.com/what-is-my-ip-address.php") :: Nil
      ).resolvePlain(spooky)

      results(0).text1("h1")
    }

    assert(newIP !== noProxyIP2)
  }
}
