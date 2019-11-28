package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.dsl.{Routing, Routings}
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.mavlink.MAVLink

/**
  * Created by peng on 30/11/16.
  */
class LinkFactoriesSuite extends SpookyEnvFixture {

  def canCreate(factory: Routing, link: Link): Boolean = {

    val dryRun = factory.apply(link.uav)
    val result = link.sameFactoryWith(dryRun)

    result
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    Link.registered.values.foreach(_.tryClean())
  }

  override def afterEach(): Unit = {
    assert(Link.registered.size == 1) // ensure that canCreate won't leave zombie link(s)
    super.afterEach()
  }

  it("NoProxy can create link without proxy") {

    val factory = Routings.Direct()
    val link = MAVLink(UAV(Seq("dummy")), Nil)().register(spooky, factory)
    assert(canCreate(factory, link))
  }

  it("NoProxy can create link with proxy that has no GCS out") {

    val factory = Routings.Direct()
    val link = MAVLink(UAV(Seq("dummy")), Seq("localhost:80"))().register(spooky, factory)
    assert(canCreate(factory, link))
  }

  it("NoProxy can create link with proxy that has 1 GCS outs") {

    val factory = Routings.Direct()
    val link = MAVLink(UAV(Seq("dummy")), Seq("localhost:80"), Seq("localhost:14550"))()
      .register(spooky, factory)
    assert(!canCreate(factory, link))
  }

  it("ForkToGCS cannot create link without proxy") {

    val factory = Routings.Forked()
    val link = MAVLink(UAV(Seq("dummy")), Nil)().register(spooky, factory)
    assert(!canCreate(factory, link))
  }

  it("ForkToGCS can create link with proxy that has identical GCS outs") {

    val factory = Routings.Forked()
    val link = MAVLink(UAV(Seq("dummy")), Seq("localhost:80"), Seq("udp:localhost:14550"))()
      .register(spooky, factory)
    assert(canCreate(factory, link))
  }

  it("ForkToGCS cannot create link with proxy that has different GCS outs") {

    val factory = Routings.Forked()
    val link = MAVLink(UAV(Seq("dummy")), Seq("localhost:80"))().register(spooky, factory)
    assert(!canCreate(factory, link))
  }
}
