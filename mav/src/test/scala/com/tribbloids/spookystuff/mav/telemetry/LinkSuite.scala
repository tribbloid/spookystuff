package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.mav.APMSimFixture
import com.tribbloids.spookystuff.session.DriverSession

/**
  * Created by peng on 12/11/16.
  */
class LinkSuite extends APMSimFixture {

  test("Link.uri should be identical to endpoint if without proxy") {
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
        result
    }
      .collect()
    uris.mkString("\n").shouldBe (
      """
        |Some(tcp:localhost:5810)
        |Some(tcp:localhost:5770)
        |Some(tcp:localhost:5820)
        |Some(tcp:localhost:5800)
        |Some(tcp:localhost:5760)
        |Some(tcp:localhost:5790)
        |Some(tcp:localhost:5830)
        |Some(tcp:localhost:5780)
      """.stripMargin,
      sort = true
    )
    assert(uris.length == uris.distinct.length)
  }

  //  val defaultProxyFactory = ProxyFactories.Default()
  test("Proxy should use different ports for DroneKit communications") {
    val spooky = this.spooky
    val proxyFactory = ProxyFactories.Default()
    val linkRDD = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new DriverSession(spooky)
        val link = Link.getOrCreate(
          Seq(endpoint),
          proxyFactory,
          session.getOrProvisionPythonDriver
        )
        link
    }
    val outs = linkRDD.map {
      link =>
        link.proxy.get.outs
    }
      .collect()
    outs.mkString("\n").shouldBe(
      """
        |List(udp:localhost:12014, udp:localhost:14550)
        |List(udp:localhost:12015, udp:localhost:14550)
        |List(udp:localhost:12016, udp:localhost:14550)
        |List(udp:localhost:12017, udp:localhost:14550)
        |List(udp:localhost:12018, udp:localhost:14550)
        |List(udp:localhost:12019, udp:localhost:14550)
        |List(udp:localhost:12020, udp:localhost:14550)
        |List(udp:localhost:12021, udp:localhost:14550)
      """.stripMargin,
      sort = true
    )
  }

  test("Link can be reused if requested in the same python driver") {
    val spooky = this.spooky
    val links = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new DriverSession(spooky)
        val link1 = Link.getOrCreate(
          Seq(endpoint),
          ProxyFactories.NoProxy,
          session.getOrProvisionPythonDriver
        )
        link1.Py(session).uri.strOpt.foreach(println)
        val link2 = Link.getOrCreate(
          Seq(endpoint),
          ProxyFactories.NoProxy,
          session.getOrProvisionPythonDriver
        )
        link1 -> link2
    }
      .collect()
    links.foreach {
      tuple =>
        assert(tuple._1 == tuple._2)
    }
  }

  test("Link can be reused if previous python driver is cleaned") {
    val spooky = this.spooky
    val links1 = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new DriverSession(spooky)
        val link = Link.getOrCreate(
          Seq(endpoint),
          ProxyFactories.NoProxy,
          session.getOrProvisionPythonDriver
        )
        link
    }
      .collect()

    val links2 = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new DriverSession(spooky)
        val link = Link.getOrCreate(
          Seq(endpoint),
          ProxyFactories.NoProxy,
          session.getOrProvisionPythonDriver
        )
        link
    }
      .collect()

    assert(links1.toSet == links2.toSet)
  }

  test("Link.uri should be identical to first out of the proxy if provided") {

    val spooky = this.spooky
    val uris = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val proxyFactory = ProxyFactories.Default()
        val session = new DriverSession(spooky)
        val link = Link.getOrCreate(
          Seq(endpoint),
          proxyFactory,
          session.getOrProvisionPythonDriver
        )
        val result = link.Py(session).uri.strOpt
        result
    }
      .collect()
    uris.mkString("\n").shouldBe(
      """
        |Some(udp:localhost:12014)
        |Some(udp:localhost:12015)
        |Some(udp:localhost:12016)
        |Some(udp:localhost:12017)
        |Some(udp:localhost:12018)
        |Some(udp:localhost:12019)
        |Some(udp:localhost:12020)
        |Some(udp:localhost:12021)
      """.stripMargin,
      sort = true
    )
  }
}
