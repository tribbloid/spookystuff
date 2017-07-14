package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.session.WebProxySetting

import scala.util.{Random => SR}

//sealed abstract class WebProxyFactory extends (() => WebProxySetting) with Serializable

object WebProxyFactories {

  case object NoProxy extends WebProxyFactory {
    override def apply(): WebProxySetting = null
  }

  case object Tor extends WebProxyFactory {
    def apply() = WebProxySetting("127.0.0.1", 9050, "socks5")
  }

  case class Random(proxies: Seq[WebProxySetting]) extends WebProxyFactory {
    def apply() = proxies(SR.nextInt(proxies.size))
  }
}