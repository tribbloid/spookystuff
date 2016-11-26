package com.tribbloids.spookystuff.mav

/**
  * Created by peng on 12/11/16.
  */
package object telemetry {

  abstract class ProxyFactory extends (Endpoint => Option[Proxy]) {

//    def unapply(link: Link): Boolean
    //TODO: this is kind of weird, is unapply supposed to be a mirror of apply?
  }
}
