package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.Delay.RandomDelay
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.testutils.{LocalOnly, RemoteDocsFixture, SpookyBaseSpec}
import org.scalatest.Tag

import java.sql.Timestamp
import scala.concurrent.duration

object WgetSpec {

  case class Sample(A: String, B: Timestamp)
}

class WgetSpec extends SpookyBaseSpec {

  import com.tribbloids.spookystuff.dsl.*

  lazy val resources: RemoteDocsFixture.type = RemoteDocsFixture
  import resources.*

  def wget(uri: String): Action = Wget(uri)

  lazy val noProxyIP: String = {
    spooky.confUpdate(_.copy(webProxy = WebProxyFactory.NoProxy))

    getIP()
  }

  Seq(
    "http" -> HTTP_IP_URL,
    "https" -> HTTPS_IP_URL
  ).foreach { tuple =>
    it(s"use TOR socks5 proxy for ${tuple._1} wget", Tag(classOf[LocalOnly].getCanonicalName)) {

      val newIP = {

        spooky.confUpdate(_.copy(webProxy = WebProxyFactory.Tor))

        getIP(tuple._2)
      }

      assert(newIP !== null)
      assert(newIP !== "")
      assert(newIP !== noProxyIP)
    }

    it(s"revert from TOR socks5 proxy for ${tuple._1} wget", Tag(classOf[LocalOnly].getCanonicalName)) {

      val newIP = {
        spooky.confUpdate(_.copy(webProxy = WebProxyFactory.Tor))

        getIP(tuple._2)
      }

      val noProxyIP2 = {
        spooky.confUpdate(_.copy(webProxy = WebProxyFactory.NoProxy))

        getIP(tuple._2)
      }

      assert(newIP !== noProxyIP2)
    }
  }

  def getIP(url: String = HTTP_IP_URL): String = {
    val results = wget(url).fetch(spooky)

    results.head.asInstanceOf[Doc].code.get
  }

  // TODO: add canonized URI check
  it("wget should encode malformed url") {
    spooky.confUpdate(_.copy(webProxy = WebProxyFactory.NoProxy))

    val results = wget("https://www.google.com/?q=giant robot").fetch(spooky)

    assert(results.size === 1)
    val doc = results.head.asInstanceOf[Doc]
    assert(doc.uri.contains("?q=giant+robot") || doc.uri.contains("?q=giant%20robot"))
  }

  // TODO: find a new way to test it!
  //  test("wget should encode redirection to malformed url") {
  //
  //    spooky.conf.proxy = ProxyFactories.NoProxy
  //
  //    val url = "http://www.sigmaaldrich.com/catalog/search/SearchResultsPage?Query=%3Ca+href%3D%22%2Fcatalog%2Fsearch%3Fterm%3D81-25-4%26interface%3DCAS+No.%26N%3D0%26mode%3Dpartialmax%26lang%3Den%26region%3DUS%26focus%3Dproduct%22%3E81-25-4%3C%2Fa%3E&Scope=CASSearch&btnSearch.x=1"
  //
  //    val results = (
  //      Wget(url) :: Nil
  //      ).fetch(spooky)
  //
  //    assert(results.size === 1)
  //    val page = results.head.asInstanceOf[Page]
  //    assert(page.uri.contains("www.sigmaaldrich.com/catalog/AdvancedSearchPage"))
  //  }

  // TODO: find a new way to test it!
  //  test("wget should correct redirection to relative url path") {
  //    spooky.conf.proxy = ProxyFactories.NoProxy
  //
  //    val results = (
  //      wget("http://www.sigmaaldrich.com/etc/controller/controller-page.html?TablePage=17193175") :: Nil
  //      ).fetch(spooky)
  //
  //    assert(results.size === 1)
  //    val page = results.head.asInstanceOf[Page]
  //    assert(page.findAll("title").head.text.get.contains("Sigma-Aldrich"))
  //    assert(page.uri.contains("www.sigmaaldrich.com/labware"))
  //  }

  // TODO: how to simulate circular redirection?
  //  test("wget should smoothly fail on circular redirection") {
  //    spooky.conf.proxy = ProxyFactories.NoProxy
  //
  //    val results = (
  //      wget("http://www.perkinelmer.ca/en-ca/products/consumables-accessories/integrated-solutions/for-thermo-scientific-gcs/default.xhtml") :: Nil
  //      ).fetch(spooky)
  //
  //    assert(results.size === 1)
  //    assert(results.head.isInstanceOf[NoPage])
  //  }

  it("output of wget should not include session's backtrace") {
    spooky.confUpdate(_.copy(webProxy = WebProxyFactory.NoProxy))

    import duration.*

    val results = (
      RandomDelay(1.seconds, 2.seconds) +> wget(HTML_URL)
    ).fetch(spooky)

    assert(results.size === 1)
    assert(results.head.uid.backtrace.last == wget(HTML_URL))
  }

  // TODO: how to simulate a PKIX exception page?
  //  test("wget should handle PKIX exception") {
  //    spooky.conf.proxy = ProxyFactories.NoProxy
  //
  //    val results = List(
  //      wget("https://www.canadacompany.ca/en/")
  //    ).fetch(spooky)
  //  }

//  val classes = Seq(
//    classOf[Wget],
//    classOf[Visit],
//    classOf[Snapshot]
//  )
//
//  classes.foreach {
//    clazz =>
//      val name = clazz.getCanonicalName
//
//      test(s"$name.serialVersionUID should be generated properly") {
//        val expected = SpookyUtils.hash(clazz)
//        val actual = java.io.ObjectStreamClass.lookup(clazz).getSerialVersionUID
//        assert(expected == actual)
//      }
//  }
}
