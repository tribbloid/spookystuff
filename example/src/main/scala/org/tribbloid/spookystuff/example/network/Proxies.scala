package org.tribbloid.spookystuff.example.network

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.example.ProxyFeedCore

/**
* Created by peng on 9/7/14.
*/
object Proxies extends ProxyFeedCore {

  override def doMain(spooky: SpookyContext) = proxyRDD
}