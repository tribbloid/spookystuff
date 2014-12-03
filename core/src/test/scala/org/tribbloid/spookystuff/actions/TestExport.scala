package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.{dsl, SparkEnvSuite}
import org.tribbloid.spookystuff.dsl.TorProxyFactory
import dsl._

/**
 * Created by peng on 11/6/14.
 */
class TestExport extends SparkEnvSuite {

  lazy val noProxyIP = {
      spooky.proxy = () => null

      val results = Trace(
        Wget("http://www.whatsmyuseragent.com/") :: Nil
      ).resolvePlain(spooky)

      results(0)("h3.info").texts.head
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

      results(0)("h3.info").texts.head
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

      results(0)("h1").texts.head
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

      results(0)("h3.info").texts.head
    }

    val noProxyIP2 = {
      spooky.proxy = () => null

      val results = Trace(
        Wget("http://www.whatsmyuseragent.com/") :: Nil
      ).resolvePlain(spooky)

      results(0)("h3.info").texts.head
    }

    assert(newIP !== noProxyIP2)
  }

  test("revert proxy setting for https wget") {

    val newIP = {
      spooky.proxy = TorProxyFactory

      val results = Trace(
        Wget("https://www.astrill.com/what-is-my-ip-address.php") :: Nil
      ).resolvePlain(spooky)

      results(0)("h1").texts.head
    }

    val noProxyIP2 = {
      spooky.proxy = () => null

      val results = Trace(
        Wget("https://www.astrill.com/what-is-my-ip-address.php") :: Nil
      ).resolvePlain(spooky)

      results(0)("h1").texts.head
    }

    assert(newIP !== noProxyIP2)
  }
}
