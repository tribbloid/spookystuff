package org.tribbloid.spookystuff.actions

import org.scalatest.tags.Retryable
import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.pages.{NoPage, Page}

import scala.concurrent.duration

/**
 * Created by peng on 11/6/14.
 */
@Retryable
class TestWget extends SpookyEnvSuite {

  lazy val noProxyIP = {
    spooky.conf.proxy = ProxyFactories.NoProxy

    val results = (
      Wget("http://www.whatsmyuseragent.com/") :: Nil
      ).resolve(spooky)

    results.head.asInstanceOf[Page].children("h3.info").texts.head
  }

  test("use TOR socks5 proxy for http wget") {

    val newIP = {
      spooky.conf.proxy = ProxyFactories.Tor

      val results = (
        Wget("http://www.whatsmyuseragent.com/") :: Nil
        ).resolve(spooky)

      results.head.asInstanceOf[Page].children("h3.info").texts.head
    }

    assert(newIP !== null)
    assert(newIP !== "")
    assert(newIP !== noProxyIP)
  }

  //TODO: find a test site for https!
  test("use TOR socks5 proxy for https wget") {

    val newIP = {
      spooky.conf.proxy = ProxyFactories.Tor

      val results = (
        Wget("https://www.astrill.com/what-is-my-ip-address.php") :: Nil
        ).resolve(spooky)

      results.head.asInstanceOf[Page].children("h1").texts.head
    }

    assert(newIP !== null)
    assert(newIP !== "")
    assert(newIP !== noProxyIP)
  }

  test("revert proxy setting for http wget") {

    val newIP = {
      spooky.conf.proxy = ProxyFactories.Tor

      val results = (
        Wget("http://www.whatsmyuseragent.com/") :: Nil
        ).resolve(spooky)
      Actions
      results.head.asInstanceOf[Page].children("h3.info").texts.head
    }

    val noProxyIP2 = {
      spooky.conf.proxy = ProxyFactories.NoProxy

      val results = (
        Wget("http://www.whatsmyuseragent.com/") :: Nil
        ).resolve(spooky)

      results.head.asInstanceOf[Page].children("h3.info").texts.head
    }

    assert(newIP !== noProxyIP2)
  }

  test("revert proxy setting for https wget") {

    val newIP = {
      spooky.conf.proxy = ProxyFactories.Tor

      val results = (
        Wget("https://www.astrill.com/what-is-my-ip-address.php") :: Nil
        ).resolve(spooky)

      results.head.asInstanceOf[Page].children("h1").texts.head
    }

    val noProxyIP2 = {
      spooky.conf.proxy = ProxyFactories.NoProxy

      val results = (
        Wget("https://www.astrill.com/what-is-my-ip-address.php") :: Nil
        ).resolve(spooky)

      results.head.asInstanceOf[Page].children("h1").texts.head
    }

    assert(newIP !== noProxyIP2)
  }

  test("wget should encode malformed url 1") {
    spooky.conf.proxy = ProxyFactories.NoProxy

    val results = (
      Wget("http://www.sigmaaldrich.com/catalog/search?term=38183-12-9&interface=CAS No.&N=0&mode=partialmax&lang=en&region=US&focus=product",hasTitle = false) :: Nil
      ).resolve(spooky)

    assert(results.size === 1)
    results.head.asInstanceOf[Page]
  }

  test("wget should encode malformed url 2") {
    spooky.conf.proxy = ProxyFactories.NoProxy
    spooky.conf.userAgent = () => "Wget/1.15 (linux-gnu)"
    spooky.conf.headers= () => Map(
      "Accept" -> "*/*",
      "Connection" -> "Keep-Alive"
    )

    val results = (
      Wget("http://www.perkinelmer.ca/Catalog/Gallery.aspx?ID=Mass Spectrometry [GC/MS and ICP-MS]&PID=Gas Chromatography Mass Spectrometry Consumables&refineCat=Technology&N=172 139 78928 4293910906&TechNVal=4293910906",hasTitle = false) :: Nil
      ).resolve(spooky)

    assert(results.size === 1)
    results.head.asInstanceOf[Page]
  }

  test("wget should encode redirection to malformed url") {

    spooky.conf.proxy = ProxyFactories.NoProxy

    val results = (
      Wget("http://www.sigmaaldrich.com/catalog/search/SearchResultsPage?Query=%3Ca+href%3D%22%2Fcatalog%2Fsearch%3Fterm%3D81-25-4%26interface%3DCAS+No.%26N%3D0%26mode%3Dpartialmax%26lang%3Den%26region%3DUS%26focus%3Dproduct%22%3E81-25-4%3C%2Fa%3E&Scope=CASSearch&btnSearch.x=1",hasTitle = false) :: Nil
      ).resolve(spooky)

    assert(results.size === 1)
    results.head.asInstanceOf[Page]
  }

  test("wget should correct redirection to relative url path") {
    spooky.conf.proxy = ProxyFactories.NoProxy

    val results = (
      Wget("http://www.sigmaaldrich.com/etc/controller/controller-page.html?TablePage=17193175",hasTitle = false) :: Nil
      ).resolve(spooky)

    assert(results.size === 1)
    assert(results.head.asInstanceOf[Page].children("title").head.text.get.contains("Sigma-Aldrich"))
  }

  test("wget should smoothly fail on circular redirection") {
    spooky.conf.proxy = ProxyFactories.NoProxy

    val results = (
      Wget("http://www.perkinelmer.ca/en-ca/products/consumables-accessories/integrated-solutions/for-thermo-scientific-gcs/default.xhtml",hasTitle = false) :: Nil
      ).resolve(spooky)

    assert(results.size === 1)
    assert(results.head.isInstanceOf[NoPage])
  }

  test("output of wget should not include session's backtrace") {
    spooky.conf.proxy = ProxyFactories.NoProxy

    import duration._

    val results = (
      RandomDelay(10.seconds, 20.seconds)
        :: Wget("http://www.wikipedia.org")
        :: Nil
      ).resolve(spooky)

    assert(results.size === 1)
    assert(results.head.uid.backtrace.self == Wget("http://www.wikipedia.org") :: Nil)
  }
}