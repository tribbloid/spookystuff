package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.session.ProxySetting

import scala.util.{Random => SR}

abstract class ProxyFactory extends (() => ProxySetting) with Serializable

object ProxyFactories {

  object NoProxy extends ProxyFactory {
    override def apply(): ProxySetting = null
  }

  object Tor extends ProxyFactory {
    def apply() = ProxySetting("127.0.0.1", 9050, "socks5")
  }

  case class Random(proxies: Seq[ProxySetting]) extends ProxyFactory {
    def apply() = proxies(SR.nextInt(proxies.size))
  }
}