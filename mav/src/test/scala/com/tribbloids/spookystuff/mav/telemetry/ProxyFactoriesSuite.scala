package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.SpookyEnvFixture

/**
  * Created by peng on 30/11/16.
  */
class ProxyFactoriesSuite extends SpookyEnvFixture {

  test("NoProxy can create link without proxy") {

    val link = Link(Endpoint(Seq("dummy")), None)(spooky)
    val factory = ProxyFactories.NoProxy
    assert(factory.canCreate(link))
  }

  test("NoProxy can create link with proxy that has no GCS out") {

    val link = Link(Endpoint(Seq("dummy")), Some(Proxy("dummy", Seq("localhost:80"))))(spooky)
    val factory = ProxyFactories.NoProxy
    assert(factory.canCreate(link))
  }

  test("NoProxy can create link with proxy that has 1 GCS outs") {

    val link = Link(Endpoint(Seq("dummy")), Some(Proxy("dummy", Seq("localhost:80", "localhost:14550"))))(spooky)
    val factory = ProxyFactories.NoProxy
    assert(!factory.canCreate(link))
  }

  test("ForkToGCS cannot create link without proxy") {

    val link = Link(Endpoint(Seq("dummy")), None)(spooky)
    val factory = ProxyFactories.ForkToGCS()
    assert(!factory.canCreate(link))
  }

  test("ForkToGCS can create link with proxy that has identical GCS outs") {

    val link = Link(Endpoint(Seq("dummy")), Some(Proxy("dummy", Seq("localhost:80", "udp:localhost:14550"))))(spooky)
    val factory = ProxyFactories.ForkToGCS()
    assert(factory.canCreate(link))
  }

  test("ForkToGCS cannot create link with proxy that has different GCS outs") {

    val link = Link(Endpoint(Seq("dummy")), Some(Proxy("dummy", Seq("localhost:80"))))(spooky)
    val factory = ProxyFactories.ForkToGCS()
    assert(!factory.canCreate(link))
  }
}
