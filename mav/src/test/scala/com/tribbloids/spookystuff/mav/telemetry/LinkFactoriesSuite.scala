package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.mav.dsl.{LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.mav.system.Drone
import com.tribbloids.spookystuff.mav.telemetry.mavlink.MAVLink

/**
  * Created by peng on 30/11/16.
  */
class LinkFactoriesSuite extends SpookyEnvFixture {

  def canCreate(factory: LinkFactory, link: Link): Boolean = {

    val dryRun = factory.apply(link.drone)
    val result = link.coFactory(dryRun)

    result
  }

  override def setUp(): Unit = {
    super.setUp()
    Link.existing.values.foreach(_.tryClean())
  }

  override def tearDown(): Unit = {
    assert(Link.existing.size == 1) // ensure that canCreate won't leave zombie link(s)
    super.tearDown()
  }

  test("NoProxy can create link without proxy") {

    val factory = LinkFactories.Direct
    val link = MAVLink(Drone(Seq("dummy")), Nil).setContext(spooky, factory)
    assert(canCreate(factory, link))
  }

  test("NoProxy can create link with proxy that has no GCS out") {

    val factory = LinkFactories.Direct
    val link = MAVLink(Drone(Seq("dummy")), Seq("localhost:80")).setContext(spooky, factory)
    assert(canCreate(factory, link))
  }

  test("NoProxy can create link with proxy that has 1 GCS outs") {

    val factory = LinkFactories.Direct
    val link = MAVLink(Drone(Seq("dummy")), Seq("localhost:80"), Seq("localhost:14550"))
      .setContext(spooky, factory)
    assert(!canCreate(factory, link))
  }

  test("ForkToGCS cannot create link without proxy") {

    val factory = LinkFactories.ForkToGCS()
    val link = MAVLink(Drone(Seq("dummy")), Nil).setContext(spooky, factory)
    assert(!canCreate(factory, link))
  }

  test("ForkToGCS can create link with proxy that has identical GCS outs") {

    val factory = LinkFactories.ForkToGCS()
    val link = MAVLink(Drone(Seq("dummy")), Seq("localhost:80"), Seq("udp:localhost:14550"))
      .setContext(spooky, factory)
    assert(canCreate(factory, link))
  }

  test("ForkToGCS cannot create link with proxy that has different GCS outs") {

    val factory = LinkFactories.ForkToGCS()
    val link = MAVLink(Drone(Seq("dummy")), Seq("localhost:80")).setContext(spooky, factory)
    assert(!canCreate(factory, link))
  }
}
