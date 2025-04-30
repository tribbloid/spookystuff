package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.testutils.{LocalOnly, RemoteDocsFixture, SpookyBaseSpec}
import org.scalatest.Tag

object WgetWithTORSpec {}

class WgetWithTORSpec extends SpookyBaseSpec {

  import com.tribbloids.spookystuff.dsl.*

  val resources: RemoteDocsFixture.type = RemoteDocsFixture
  import resources.*

  // TODO: add sanity check for local TOR service

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
}
