package com.tribbloids.spookystuff.example.network

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.example.ProxyFeedCore

/**
* Created by peng on 9/7/14.
*/
object Proxies extends ProxyFeedCore {

  override def doMain(spooky: SpookyContext) = proxyRDD
}