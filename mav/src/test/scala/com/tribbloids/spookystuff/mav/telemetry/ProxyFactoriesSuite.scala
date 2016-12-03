package com.tribbloids.spookystuff.mav.telemetry

import org.scalatest.FunSuite

/**
  * Created by peng on 30/11/16.
  */
class ProxyFactoriesSuite extends FunSuite {

  test("NoProxy can create link without proxy") {

    val link = Link(Endpoint(Seq("dummy")), None)
    val factory = ProxyFactories.NoProxy
    assert(factory.canCreate(link))
  }

  test("NoProxy can create link with proxy that has no GCS out") {

    val link = Link(Endpoint(Seq("dummy")), Some(Proxy("dummy", Seq("localhost:80"))))
    val factory = ProxyFactories.NoProxy
    assert(factory.canCreate(link))
  }

  test("NoProxy can create link with proxy that has 1 GCS outs") {

    val link = Link(Endpoint(Seq("dummy")), Some(Proxy("dummy", Seq("localhost:80", "localhost:14550"))))
    val factory = ProxyFactories.NoProxy
    assert(!factory.canCreate(link))
  }

  test("ForkToGCS cannot create link without proxy") {

    val link = Link(Endpoint(Seq("dummy")), None)
    val factory = ProxyFactories.ForkToGCS()
    assert(!factory.canCreate(link))
  }

  test("ForkToGCS can create link with proxy that has identical GCS outs") {

    val link = Link(Endpoint(Seq("dummy")), Some(Proxy("dummy", Seq("localhost:80", "udp:localhost:14550"))))
    val factory = ProxyFactories.ForkToGCS()
    assert(factory.canCreate(link))
  }

  test("ForkToGCS cannot create link with proxy that has different GCS outs") {

    val link = Link(Endpoint(Seq("dummy")), Some(Proxy("dummy", Seq("localhost:80"))))
    val factory = ProxyFactories.ForkToGCS()
    assert(!factory.canCreate(link))
  }
}
