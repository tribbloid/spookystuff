package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.pages.Page
import org.tribbloid.spookystuff.{dsl, SpookyEnvSuite}
import org.tribbloid.spookystuff.dsl.TorProxyFactory
import dsl._

/**
 * Created by peng on 11/6/14.
 */
class TestExport extends SpookyEnvSuite {

  lazy val noProxyIP = {
      spooky.proxy = () => null

      val results = Trace(
        Wget("http://www.whatsmyuseragent.com/") :: Nil
      ).resolve(spooky)

      results(0).asInstanceOf[Page].children("h3.info").texts.head
    }

  test("use TOR socks5 proxy for http wget") {

    val newIP = {
      spooky.proxy = TorProxyFactory

      val results = Trace(
        Wget("http://www.whatsmyuseragent.com/") :: Nil
      ).resolve(spooky)

      results(0).asInstanceOf[Page].children("h3.info").texts.head
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
      ).resolve(spooky)

      results(0).asInstanceOf[Page].children("h1").texts.head
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
      ).resolve(spooky)

      results(0).asInstanceOf[Page].children("h3.info").texts.head
    }

    val noProxyIP2 = {
      spooky.proxy = () => null

      val results = Trace(
        Wget("http://www.whatsmyuseragent.com/") :: Nil
      ).resolve(spooky)

      results(0).asInstanceOf[Page].children("h3.info").texts.head
    }

    assert(newIP !== noProxyIP2)
  }

  test("revert proxy setting for https wget") {

    val newIP = {
      spooky.proxy = TorProxyFactory

      val results = Trace(
        Wget("https://www.astrill.com/what-is-my-ip-address.php") :: Nil
      ).resolve(spooky)

      results(0).asInstanceOf[Page].children("h1").texts.head
    }

    val noProxyIP2 = {
      spooky.proxy = () => null

      val results = Trace(
        Wget("https://www.astrill.com/what-is-my-ip-address.php") :: Nil
      ).resolve(spooky)

      results(0).asInstanceOf[Page].children("h1").texts.head
    }

    assert(newIP !== noProxyIP2)
  }
}
