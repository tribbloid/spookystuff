package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.session.WebProxySetting

object WebProxyFactories {

  case object NoProxy extends WebProxyFactory {
    override def apply(v: Unit): WebProxySetting = null
  }

  case object Tor extends WebProxyFactory {
    def apply(v: Unit): WebProxySetting = WebProxySetting("127.0.0.1", 9050, "socks5")
  }

//  case class Random(proxies: Seq[WebProxySetting]) extends WebProxyFactory {
//    def apply(): WebProxySetting = proxies(SR.nextInt(proxies.size))
//  }
}
