package org.tribbloid.spookystuff.dsl

import org.openqa.selenium.Proxy

import scala.util.Random

/**
 * Created by peng on 11/4/14.
 */
case class ProxySetting (
                       addr: String,
                       port: Int,
                       protocol: String
                       ) {

  lazy val toSeleniumProxy: Proxy = {
    val seleniumProxy: Proxy = new Proxy
    seleniumProxy.setProxyType(Proxy.ProxyType.MANUAL)
    val proxyStr: String = s"$addr:$port"
    seleniumProxy.setHttpProxy(proxyStr)
    seleniumProxy.setSslProxy(proxyStr)
    seleniumProxy.setSocksProxy(proxyStr)
    seleniumProxy
  }
}

abstract class ProxyFactory extends (() => ProxySetting) with Serializable

object NoProxyFactory extends ProxyFactory {
  override def apply(): ProxySetting = null
}

object TorProxyFactory extends ProxyFactory {
  def apply() = ProxySetting("127.0.0.1", 9050, "socks5")
}

case class RandomProxyFactory(proxies: Seq[ProxySetting]) extends ProxyFactory {
  def apply() = proxies(Random.nextInt(proxies.size))
}