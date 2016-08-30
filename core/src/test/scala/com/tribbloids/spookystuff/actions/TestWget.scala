package com.tribbloids.spookystuff.actions

import java.sql.Timestamp

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.rdd.FetchedDataset
import com.tribbloids.spookystuff.testutils.Tor
import org.scalatest.Tag
import org.scalatest.tags.Retryable

import scala.concurrent.duration

object TestWget {

  case class Sample(A: String, B: Timestamp)
}

@Retryable
class TestWget extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.dsl._

  def wget(uri: String): Action = Wget(uri)

  lazy val noProxyIP = {
    spooky.conf.proxy = ProxyFactories.NoProxy

    val results = (
      //TODO: may not be stable, alternatives are http://www.whatsmyua.com/ and http://user-agent.me/
      wget("http://www.whatsmyuseragent.com/") :: Nil
      ).fetch(spooky)

    results.head.asInstanceOf[Doc].findAll("div.info-box span").texts.head
  }

  //TODO: find a test site for http!
  //  test("use TOR socks5 proxy for http wget") {
  //
  //    val newIP = {
  //      spooky.conf.proxy = ProxyFactories.Tor
  //
  //      val results = (
  //        wget("http://www.whatsmyuseragent.com/") :: Nil
  //        ).fetch(spooky)
  //
  //      results.head.asInstanceOf[Page].findAll("h3.info").texts.head
  //    }
  //
  //    assert(newIP !== null)
  //    assert(newIP !== "")
  //    assert(newIP !== noProxyIP)
  //  }

  test("use TOR socks5 proxy for https wget", Tag(classOf[Tor].getCanonicalName)) {

    val newIP = {
      spooky.conf.proxy = ProxyFactories.Tor

      getIP
    }

    assert(newIP !== null)
    assert(newIP !== "")
    assert(newIP !== noProxyIP)
  }

  //  test("revert proxy setting for http wget") {
  //
  //    val newIP = {
  //      spooky.conf.proxy = ProxyFactories.Tor
  //
  //      val results = (
  //        wget("http://www.whatsmyuseragent.com/") :: Nil
  //        ).fetch(spooky)
  //      Actions
  //      results.head.asInstanceOf[Page].findAll("h3.info").texts.head
  //    }
  //
  //    val noProxyIP2 = {
  //      spooky.conf.proxy = ProxyFactories.NoProxy
  //
  //      val results = (
  //        wget("http://www.whatsmyuseragent.com/") :: Nil
  //        ).fetch(spooky)
  //
  //      results.head.asInstanceOf[Page].findAll("h3.info").texts.head
  //    }
  //
  //    assert(newIP !== noProxyIP2)
  //  }

  def getIP: String = {
    val results = (
      wget(IP_URL) :: Nil
      )
      .fetch(spooky)

    results.head.asInstanceOf[Doc].code.get
  }

  test("revert from TOR socks5 proxy for https wget", Tag(classOf[Tor].getCanonicalName)) {

    val newIP = {
      spooky.conf.proxy = ProxyFactories.Tor

      getIP
    }

    val noProxyIP2 = {
      spooky.conf.proxy = ProxyFactories.NoProxy

      getIP
    }

    assert(newIP !== noProxyIP2)
  }

  //TODO: add canonized URI check
  test("wget should encode malformed url") {
    spooky.conf.proxy = ProxyFactories.NoProxy

    val results = (
      wget("https://www.google.com/?q=giant robot") :: Nil
      ).fetch(spooky)

    assert(results.size === 1)
    val doc = results.head.asInstanceOf[Doc]
    assert(doc.uri.contains("?q=giant+robot") || doc.uri.contains("?q=giant%20robot" ))
  }

  //TODO: find a new way to test it!
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

  //TODO: find a new way to test it!
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

  //TODO: how to simulate circular redirection?
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

  test("output of wget should not include session's backtrace") {
    spooky.conf.proxy = ProxyFactories.NoProxy

    import duration._

    val results = (
      RandomDelay(1.seconds, 2.seconds)
        :: wget("http://www.wikipedia.org")
        :: Nil
      ).fetch(spooky)

    assert(results.size === 1)
    assert(results.head.uid.backtrace.children.last == wget("http://www.wikipedia.org"))
  }

  //TODO: how to simulate a PKIX exception page?
  //  test("wget should handle PKIX exception") {
  //    spooky.conf.proxy = ProxyFactories.NoProxy
  //
  //    val results = List(
  //      wget("https://www.canadacompany.ca/en/")
  //    ).fetch(spooky)
  //  }

  test("wget.interpolate should not overwrite each other") {
    val wget = Wget(
      'A
    ) waybackTo 'B.typed[Timestamp]

    val rows = 1 to 5 map {
      i =>
        TestWget.Sample("http://dummy.com" + i, new Timestamp(i * 100000))
    }
    require(rows.map(_.B.getTime).distinct.size == rows.size)

    val df = sql.createDataFrame(sc.parallelize(rows))
    val set: FetchedDataset = spooky.create(df)

    require(set.toObjectRDD('B).collect().toSeq.map(_.asInstanceOf[Timestamp].getTime).distinct.size == rows.size)
    val fetchedRows = set.unsquashedRDD.collect()

    val interpolated = fetchedRows.map{
      fr =>
        wget.interpolate(fr, set.schema).get
    }

    assert(interpolated.distinct.length == rows.size)
    assert(interpolated.map(_.wayback).distinct.length == rows.size)
  }

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