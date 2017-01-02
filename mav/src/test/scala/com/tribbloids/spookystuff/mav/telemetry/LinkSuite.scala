package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.PyInterpreterException
import com.tribbloids.spookystuff.mav.dsl.LinkFactories
import com.tribbloids.spookystuff.mav.sim.APMSimFixture
import com.tribbloids.spookystuff.session.{Lifespan, NoPythonDriverException, Session}
import org.slf4j.LoggerFactory

object LinkSuite {

  def driverLifespan: Lifespan = new Lifespan.JVM()
}

class LinkSuite extends APMSimFixture {

  import LinkSuite._
  import com.tribbloids.spookystuff.utils.SpookyViews._

  lazy val getEndpoints: String => Seq[Endpoint] = {
    connStr =>
      Seq(Endpoint(Seq(connStr)))
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

  test("Link can create PyBinding") {

    val session = new Session(spooky, driverLifespan)
    val proxyFactory = LinkFactories.NoProxy
    val link = Link.getOrInitialize(
      getEndpoints(simConnStrs.head),
      proxyFactory,
      session
    )

    val py = link.Py(session)

    val name = py.endpoint.name.$STR.get
    val endpoint = py.endpoint.$MSG.get.prettyJSON()

    name.shouldBe(
      "DRONE"
    )
    endpoint.shouldBeLike(
      s"""
         |{
         |  "connStrs" : [ "tcp:localhost:......" ],
         |  "baudRate" : 57600,
         |  "frame" : null,
         |  "name" : "DRONE"
         |}
         |
      """.stripMargin
    )
  }

  test("Link PyBinding failed to be created won't exist in Link.existing or driverLocal") {
    val session = new Session(spooky)
    val proxyFactory = LinkFactories.NoProxy

    Link.existing.values.toList.foreach(_.clean())

    // this will fail due to lack of Python Driver
    intercept[NoPythonDriverException.type] {
      val link = Link.getOrCreate(
        getEndpoints(simConnStrs.head),
        proxyFactory,
        session
      )
        .Py(session)
    }
    assert(Link.existing.isEmpty)
    assert(Link.driverLocal.isEmpty)

    // this will fail due to non-existing endpoint
    intercept[PyInterpreterException] {
      val link = Link.getOrInitialize(
        Seq(Endpoint(Seq("dummy"))),
        proxyFactory,
        session
      )
        .Py(session)
    }
    assert(Link.existing.isEmpty)
    assert(Link.driverLocal.isEmpty)
  }

  test("Link.detectConflicts won't trigger false alarm by itself") {
    val session = new Session(spooky)
    val proxyFactory = LinkFactories.NoProxy

    val link = Link.selectAndCreate(
      getEndpoints(simConnStrs.head),
      proxyFactory,
      session
    )
      .link

    link.detectPossibleConflicts()
  }

  test("If without Proxy, Link.uri should = endpoint.connStr") {
    val spooky = this.spooky
    val proxyFactory = LinkFactories.NoProxy
    val getEndpoints = this.getEndpoints
    val connStr_URIs = simConnStrRDD.map {
      connStr =>
        val session = new Session(spooky, driverLifespan)
        val link = Link.getOrInitialize(
          getEndpoints(connStr),
          proxyFactory,
          session
        )

        link.endpoint.connStr -> link.Py(session).uri.$STR.get
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

  //  val defaultProxyFactory = ProxyFactories.Default()
  test("each Proxy for Link should use a different primary out") {
    val spooky = this.spooky
    val proxyFactory = LinkFactories.ForkToGCS()
    val getEndpoints = this.getEndpoints
    val linkRDD = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new Session(spooky, driverLifespan)
        val link = Link.getOrInitialize(
          getEndpoints(connStr),
          proxyFactory,
          session
        )
        link
    }
    val outs = linkRDD.map {
      link =>
        link.proxyOpt.get.outs
    }
      .collect()

    val expectedOuts = (0 until parallelism).map {
      i =>
        val uris = List("udp:localhost:......", "udp:localhost:14550")
        uris
    }

    outs.mkString("\n").shouldBeLike(
      expectedOuts.mkString("\n"),
      sort = true
    )
    assert(outs.distinct.length == parallelism)
  }

  test("If with Proxy, Link.uri should = Proxy.primaryOut") {

    val spooky = this.spooky
    val proxyFactory = LinkFactories.ForkToGCS()
    val getEndpoints = this.getEndpoints
    val uris = simConnStrRDD.map {
      connStr =>
        val session = new Session(spooky, driverLifespan)
        val link = Link.getOrInitialize(
          getEndpoints(connStr),
          proxyFactory,
          session
        )
        val firstOut = link.proxyOpt.get.outs.headOption
        val uri = link.Py(session).uri.$STR
        //        link.tryClean()
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
      test(s"With proxyFactory=${factory.getClass.getSimpleName}," +
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
            link1.Py(session).uri.$STR.foreach(println)
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

      test(s"With proxyFactory=${factory.getClass.getSimpleName}," +
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

class LinkSuite_SelectFromList extends LinkSuite {

  override lazy val getEndpoints = {
    val simEndpoints = this.simEndpoints
    _: String => simEndpoints
  }
}