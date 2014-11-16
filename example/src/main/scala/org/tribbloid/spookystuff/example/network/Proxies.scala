package org.tribbloid.spookystuff.example.network

import org.tribbloid.spookystuff.example.ProxyFeed

/**
* Created by peng on 9/7/14.
*/
object Proxies extends ProxyFeed {

  override def doMain() = proxyRDD
}