package com.tribbloids.spookystuff.session

import org.openqa.selenium.Proxy

/**
  * Created by peng on 11/4/14.
  */
case class WebProxySetting(
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
