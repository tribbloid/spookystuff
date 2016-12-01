package com.tribbloids.spookystuff.mav.telemetry

/**
  * Created by peng on 12/11/16.
  */
class LinkWithProxyIT extends LinkIT {

  override lazy val proxyFactory = ProxyFactories.ForkToGCS()
}
