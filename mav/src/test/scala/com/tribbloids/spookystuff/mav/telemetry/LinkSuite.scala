package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.{PyInterpreterException, SpookyEnvFixture}
import com.tribbloids.spookystuff.mav.dsl.LinkFactories
import com.tribbloids.spookystuff.mav.sim.APMSimFixture
import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.session.{Lifespan, NoPythonDriverException, Session}
import org.slf4j.LoggerFactory

object LinkSuite {

  def driverLifespan: Lifespan = new Lifespan.JVM()
}

class LinkSuite extends APMSimFixture {

  import LinkSuite._
  import com.tribbloids.spookystuff.utils.SpookyViews._

  lazy val getEndpoints: String => Seq[Drone] = {
    connStr =>
      Seq(Drone(Seq(connStr)))
  }

  override def setUp(): Unit = {

    super.setUp()
    sc.foreachComputer {
      Link.existing.values.toList.foreach(_.clean())
    }
  }

  private def mockLinkTermination() = {
    LoggerFactory.getLogger(this.getClass).info("======== Python Drivers Cleanup ========")
    sc.foreachWorker {

      Link.existing.values.foreach(_.link.validDriverToBindings.keys.foreach(_.tryClean()))
    }
    Thread.sleep(2000)
  }

  test("Link can create Binding that doesn't have any method") {

    val session = new Session(spooky, driverLifespan)
    val factory = LinkFactories.NoProxy
    val link = Link.getOrInitialize(
      getEndpoints(simConnStrs.head),
      factory,
      session
    )
    val py = link.Py(session)

    intercept[PyInterpreterException] {
      py.endpoint
    }
  }

  test("Link cannot create 2 Bindings") {

    val session = new Session(spooky, driverLifespan)
    val factory = LinkFactories.NoProxy
    val link = Link.getOrInitialize(
      getEndpoints(simConnStrs.head),
      factory,
      session
    )
    val py = link.Py(session)

    val newDriver = new PythonDriver(lifespan = driverLifespan)
    intercept[IllegalArgumentException] {
      val py = link._Py(newDriver)
    }
  }

  test("Link failed to be created won't exist in Link.existing or driverLocal") {
    val session = new Session(spooky)
    val factory = LinkFactories.NoProxy

    Link.existing.values.toList.foreach(_.clean())

    // this will fail due to lack of Python Driver
    intercept[NoPythonDriverException.type] {
      val link = Link.getOrCreate(
        getEndpoints(simConnStrs.head),
        factory,
        session
      )
        .Py(session)
    }
    assert(Link.existing.isEmpty)
    assert(Link.driverLocal.isEmpty)

    // this will fail due to non-existing endpoint
    intercept[PyInterpreterException] {
      val link = Link.getOrInitialize(
        Seq(Drone(Seq("dummy"))),
        factory,
        session
      )
        .Py(session)
    }
    assert(Link.existing.isEmpty)
    assert(Link.driverLocal.isEmpty)
  }

  test("Link.detectConflicts won't trigger false alarm by itself") {
    val session = new Session(spooky)
    val factory = LinkFactories.NoProxy

    val link = Link.selectAndCreate(
      getEndpoints(simConnStrs.head),
      factory,
      session
    )
      .link

    link.detectPortConflicts()
  }

  test("If without Proxy, Link.primary should = endpoint") {
    val spooky = this.spooky
    val factory = LinkFactories.NoProxy
    val getEndpoints = this.getEndpoints
    val connStr_URIs = simConnStrRDD.map {
      connStr =>
        val session = new Session(spooky, driverLifespan)
        val link = Link.getOrInitialize(
          getEndpoints(connStr),
          factory,
          session
        )

        link.nativeEndpoint.connStr -> link.primaryEndpoint.connStr
    }
      .collect()

    val expectedURIs = (0 until parallelism).map {
      i =>
        val port = i * 10 + 5760
        val uri = s"tcp:localhost:$port"
        uri -> uri
    }

    connStr_URIs.mkString("\n").shouldBe (
      expectedURIs.mkString("\n"),
      sort = true
    )
    assert(connStr_URIs.length == connStr_URIs.distinct.length)
  }

  //  val defaultfactory = ProxyFactories.Default()
  test("each Link Proxy should use a different primary out") {
    val spooky = this.spooky
    val factory = LinkFactories.ForkToGCS()
    val getEndpoints = this.getEndpoints
    val linkRDD = simConnStrRDD.map {
      connStr =>
        val endpoint = Drone(Seq(connStr))
        val session = new Session(spooky, driverLifespan)
        val link = Link.getOrInitialize(
          getEndpoints(connStr),
          factory,
          session
        )
        link
    }
    val outs = linkRDD.map {
      link =>
        link.proxyOpt.get.outs.mkString(",")
    }
      .collect()

    val expectedOuts = (0 until parallelism).map {
      i =>
        val uris = List("udp:localhost:......", "udp:localhost:14550").mkString(",")
        uris
    }

    outs.mkString("\n").shouldBeLike(
      expectedOuts.mkString("\n"),
      sort = true
    )
    assert(outs.distinct.length == parallelism)
  }

  test("Link Proxy.PY.stop() should not leave dangling process") {
    val spooky = this.spooky
    val factory = LinkFactories.ForkToGCS()
    val getEndpoints = this.getEndpoints
    val linkRDD = simConnStrRDD.map {
      connStr =>
        val endpoint = Drone(Seq(connStr))
        val session = new Session(spooky, driverLifespan)
        val link = Link.getOrInitialize(
          getEndpoints(connStr),
          factory,
          session
        )
        link
    }
    linkRDD.foreach {
      link =>
        val py = link.proxyOpt.get.PY
        for (i <- 1 to 2) {
          py.start()
          py.stop()
        }
    }
    sc.foreachComputer {
      SpookyEnvFixture.processShouldBeClean(Seq("mavproxy"), Seq("mavproxy"), cleanSweep = false)
    }
  }

  test("If with Proxy, Link.primary should = Proxy.outs.head") {

    val spooky = this.spooky
    val factory = LinkFactories.ForkToGCS()
    val getEndpoints = this.getEndpoints
    val uris = simConnStrRDD.map {
      connStr =>
        val session = new Session(spooky, driverLifespan)
        val link = Link.getOrInitialize(
          getEndpoints(connStr),
          factory,
          session
        )
        val firstOut = link.proxyOpt.get.outs.head
        val uri = link.primaryEndpoint.connStr
        firstOut -> uri
    }
      .collect()
    uris.foreach {
      tuple =>
        assert(tuple._1 == tuple._2)
    }
  }

  val factories = Seq(
    LinkFactories.NoProxy,
    LinkFactories.ForkToGCS()
  )

  factories.foreach {
    factory =>
      test(s"If factory=${factory.getClass.getSimpleName}," +
        s" Link created in the same python driver can be reused") {
        Link.existing.values.foreach {
          _.tryClean()
        }
        Thread.sleep(2000) //Waiting for python drivers to terminate

        val spooky = this.spooky
        val getEndpoints = this.getEndpoints
        val linkStrs = simConnStrRDD.map {
          connStr =>
            val endpoints = getEndpoints(connStr)
            val session = new Session(spooky, driverLifespan)
            val link1 = Link.getOrInitialize (
              endpoints,
              factory,
              session
            )
            link1.Py(session)
            val link2 = Link.getOrInitialize (
              endpoints,
              factory,
              session
            )
            val result = link1.toString -> link2.toString
            result
        }
          .collect()
        assert(spooky.metrics.linkCreated.value == parallelism)
        //        assert(spooky.metrics.linkDestroyed.value == 0) // not testable, refit always destroy previous link
        linkStrs.foreach {
          tuple =>
            assert(tuple._1 == tuple._2)
        }
      }

      test(s"If factory=${factory.getClass.getSimpleName}," +
        s" idle Link (with no active Python driver) can be refit for anther Python driver") {
        Link.existing.values.foreach {
          _.tryClean()
        }
        Thread.sleep(2000) //Waiting for python drivers to terminate

        val spooky = this.spooky
        val getEndpoints = this.getEndpoints
        val linkStrs1 = simConnStrRDD.map {
          connStr =>
            val session = new Session(spooky, driverLifespan)
            val link = Link.getOrInitialize(
              getEndpoints(connStr),
              factory,
              session
            )
            link.toString
        }
          .collect()

        mockLinkTermination()

        assert(spooky.metrics.linkCreated.value == parallelism)
        assert(spooky.metrics.linkDestroyed.value == 0)
        assert(Link.existing.size == parallelism)

        val livingLinkDrivers = Link.existing.values.toSeq.flatMap {
          link =>
            link.link.validDriverToBindings.keys
        }
        assert(livingLinkDrivers.isEmpty)

        val linkStrs2 = simConnStrRDD.map {
          connStr =>
            val session = new Session(spooky, driverLifespan)
            val link = Link.getOrInitialize(
              getEndpoints(connStr),
              factory,
              session
            )
            link.toString
        }
          .collect()

        assert(spooky.metrics.linkCreated.value == parallelism)
        assert(spooky.metrics.linkDestroyed.value == 0)
        linkStrs1.mkString("\n").shouldBe (
          linkStrs2.mkString("\n"),
          sort = true
        )
      }
  }
}

class LinkSuite_SelectFromFleet extends LinkSuite {

  override lazy val getEndpoints = {
    val simEndpoints = this.simEndpoints
    _: String => simEndpoints
  }
}