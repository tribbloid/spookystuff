package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.session.ProxySetting

import scala.util.{Random => SR}

abstract class AbstractProxyFactory extends (() => ProxySetting) with Serializable

object ProxyFactories {

  object NoProxy extends AbstractProxyFactory {
    override def apply(): ProxySetting = null
  }

  object Tor extends AbstractProxyFactory {
    def apply() = ProxySetting("127.0.0.1", 9050, "socks5")
  }

  case class Random(proxies: Seq[ProxySetting]) extends AbstractProxyFactory {
    def apply() = proxies(SR.nextInt(proxies.size))
  }
}