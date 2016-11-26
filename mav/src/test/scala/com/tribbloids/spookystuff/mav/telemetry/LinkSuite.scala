package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.mav.APMSimFixture
import com.tribbloids.spookystuff.session.DriverSession

/**
  * Created by peng on 12/11/16.
  */
class LinkSuite extends APMSimFixture {

//  override def parallelism = 1

  test("Link.uri should = endpoint if without proxy") {
    val spooky = this.spooky
    val uris = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new DriverSession(spooky)
        val link = Link.getOrCreate(
          Seq(endpoint),
          ProxyFactories.NoProxy,
          session.getOrProvisionPythonDriver
        )

        val result = link.Py(session).uri.strOpt
        link.tryClean()
        result
    }
      .collect()
    val expectedURIs = (0 until parallelism).map {
      i =>
        val port = i * 10 + 5760
        val uri = s"tcp:localhost:$port"
        Some(uri)
    }

    uris.mkString("\n").shouldBe (
      expectedURIs.mkString("\n"),
      sort = true
    )
    assert(uris.length == uris.distinct.length)
  }

  //  val defaultProxyFactory = ProxyFactories.Default()
  test("each Link with Proxy should use different ports for DroneKit communications") {
    val spooky = this.spooky
    val proxyFactory = ProxyFactories.ForkToGCS()
    val linkRDD = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new DriverSession(spooky)
        val link = Link.getOrCreate(
          Seq(endpoint),
          proxyFactory,
          session.getOrProvisionPythonDriver
        )
//        link.clean()
        link
    }
    val outs = linkRDD.map {
      link =>
        link.proxyOpt.get.outs
    }
      .collect()

    val expectedOuts = (0 until parallelism).map {
      i =>
        val port = i + 12014
        val uris = List(s"udp:localhost:$port", "udp:localhost:14550")
        uris
    }

    outs.mkString("\n").shouldBe(
      expectedOuts.mkString("\n"),
      sort = true
    )
  }

  test("Link.uri with Proxy should use first out of its proxy") {

    val spooky = this.spooky
    val proxyFactory = ProxyFactories.ForkToGCS()
    val uris = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new DriverSession(spooky)
        val link = Link.getOrCreate(
          Seq(endpoint),
          proxyFactory,
          session.getOrProvisionPythonDriver
        )
        val firstOut = link.proxyOpt.get.outs.headOption
        val uri = link.Py(session).uri.strOpt
        link.tryClean()
        firstOut -> uri
    }
      .collect()
    uris.foreach {
      tuple =>
        assert(tuple._1 == tuple._2)
    }
  }

  val factories = Seq(
    ProxyFactories.NoProxy,
    ProxyFactories.ForkToGCS()
  )

  factories.foreach {
    factory =>

      test(s"With ${factory.getClass.getSimpleName} proxy, Link created in the same python driver can be reused") {
        val spooky = this.spooky
        val links = simConnStrRDD.map {
          connStr =>
            val endpoint = Endpoint(Seq(connStr))
            val session = new DriverSession(spooky)
            val link1 = Link.getOrCreate (
              Seq(endpoint),
              factory,
              session.getOrProvisionPythonDriver
            )
            link1.Py(session).uri.strOpt.foreach(println)
            val link2 = Link.getOrCreate (
              Seq(endpoint),
              factory,
              session.getOrProvisionPythonDriver
            )
            val result = link1 -> link2
            result
        }
          .collect()
        links.foreach {
          tuple =>
            assert(tuple._1 == tuple._2)
        }
      }

      test(s"With ${factory.getClass.getSimpleName} proxy, idle Link (with no active Python driver) can be reused by anther Python driver") {
        val spooky = this.spooky
        val links1 = simConnStrRDD.map {
          connStr =>
            val endpoint = Endpoint(Seq(connStr))
            val session = new DriverSession(spooky)
            val link = Link.getOrCreate(
              Seq(endpoint),
              factory,
              session.getOrProvisionPythonDriver
            )
            link
        }
          .collect()

        val links2 = simConnStrRDD.map {
          connStr =>
            val endpoint = Endpoint(Seq(connStr))
            val session = new DriverSession(spooky)
            val link = Link.getOrCreate (
              Seq(endpoint),
              factory,
              session.getOrProvisionPythonDriver
            )
            link
        }
          .collect()

        assert(links1.toSet == links2.toSet)
      }
  }
}
