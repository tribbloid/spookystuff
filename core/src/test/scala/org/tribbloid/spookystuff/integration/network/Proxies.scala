package org.tribbloid.spookystuff.integration.network

import org.tribbloid.spookystuff.integration.ProxyFeed

/**
* Created by peng on 9/7/14.
*/
object Proxies extends ProxyFeed {

  override def doMain() = proxyRDD
}