package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.mav.dsl.LinkFactories

/**
  * Created by peng on 30/11/16.
  */
class LinkFactoriesSuite extends SpookyEnvFixture {

  override def tearDown(): Unit = {
    assert(Link.existing.size == 1) // ensure that canCreate won't leave zombie link(s)
    super.tearDown()
  }

  test("NoProxy can create link without proxy") {

    Link.existing.values.foreach(_.tryClean())
    val factory = LinkFactories.NoProxy
    val link = Link(Drone(Seq("dummy")), Nil).wContext(spooky, factory)
    assert(LinkFactories.canCreate(factory, link))
  }

  test("NoProxy can create link with proxy that has no GCS out") {

    Link.existing.values.foreach(_.tryClean())
    val factory = LinkFactories.NoProxy
    val link = Link(Drone(Seq("dummy")), Seq("localhost:80")).wContext(spooky, factory)
    assert(LinkFactories.canCreate(factory, link))
  }

  test("NoProxy can create link with proxy that has 1 GCS outs") {

    Link.existing.values.foreach(_.tryClean())
    val factory = LinkFactories.NoProxy
    val link = Link(Drone(Seq("dummy")), Seq("localhost:80"), Seq("localhost:14550"))
      .wContext(spooky, factory)
    assert(!LinkFactories.canCreate(factory, link))
  }

  test("ForkToGCS cannot create link without proxy") {

    Link.existing.values.foreach(_.tryClean())
    val factory = LinkFactories.ForkToGCS()
    val link = Link(Drone(Seq("dummy")), Nil).wContext(spooky, factory)
    assert(!LinkFactories.canCreate(factory, link))
  }

  test("ForkToGCS can create link with proxy that has identical GCS outs") {

    Link.existing.values.foreach(_.tryClean())
    val factory = LinkFactories.ForkToGCS()
    val link = Link(Drone(Seq("dummy")), Seq("localhost:80"), Seq("udp:localhost:14550"))
      .wContext(spooky, factory)
    assert(LinkFactories.canCreate(factory, link))
  }

  test("ForkToGCS cannot create link with proxy that has different GCS outs") {

    Link.existing.values.foreach(_.tryClean())
    val factory = LinkFactories.ForkToGCS()
    val link = Link(Drone(Seq("dummy")), Seq("localhost:80")).wContext(spooky, factory)
    assert(!LinkFactories.canCreate(factory, link))
  }
}
