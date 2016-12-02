package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.mav.sim.APMSimFixture
import com.tribbloids.spookystuff.session.Session

/**
  * Created by peng on 12/11/16.
  */
class LinkSuite extends APMSimFixture {

//  override def parallelism = 2

  test("Link.uri should = endpoint.connStr if without proxy") {
    val spooky = this.spooky
    val uris = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new Session(spooky)
        val link = Link.getOrCreate(
          Seq(endpoint),
          ProxyFactories.NoProxy,
          session
        )

        val result = link.Py(session).uri.strOpt
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
  test("each Link with Proxy should use a different primary out") {
    val spooky = this.spooky
    val proxyFactory = ProxyFactories.ForkToGCS()
    val linkRDD = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new Session(spooky)
        val link = Link.getOrCreate(
          Seq(endpoint),
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

  test("Link.uri with Proxy should = Proxy.primaryOut") {

    val spooky = this.spooky
    val proxyFactory = ProxyFactories.ForkToGCS()
    val uris = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val session = new Session(spooky)
        val link = Link.getOrCreate(
          Seq(endpoint),
          proxyFactory,
          session
        )
        val firstOut = link.proxyOpt.get.outs.headOption
        val uri = link.Py(session).uri.strOpt
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
    ProxyFactories.NoProxy,
    ProxyFactories.ForkToGCS()
  )

  factories.foreach {
    factory =>

      test(s"With ${factory.getClass.getSimpleName} proxy, Link created in the same python driver can be reused") {
        Link.existing.values.map(_._2).foreach {
          _.tryClean()
        }
        val spooky = this.spooky
        val links = simConnStrRDD.map {
          connStr =>
            val endpoint = Endpoint(Seq(connStr))
            val session = new Session(spooky)
            val link1 = Link.getOrCreate (
              Seq(endpoint),
              factory,
              session
            )
            link1.Py(session).uri.strOpt.foreach(println)
            val link2 = Link.getOrCreate (
              Seq(endpoint),
              factory,
              session
            )
            val result = link1 -> link2
            result
        }
          .collect()
        links.foreach {
          tuple =>
            assert(tuple._1 == tuple._2)
        }
        assert(spooky.metrics.linkCreated.value == parallelism)
      }

      test(s"With ${factory.getClass.getSimpleName} proxy, idle Link (with no active Python driver) can be reused by anther Python driver") {
        Link.existing.values.map(_._2).foreach {
          _.tryClean()
        }
        val spooky = this.spooky
        val links1 = simConnStrRDD.map {
          connStr =>
            val endpoint = Endpoint(Seq(connStr))
            val session = new Session(spooky)
            val link = Link.getOrCreate(
              Seq(endpoint),
              factory,
              session
            )
            link
        }
          .collect()

        val links2 = simConnStrRDD.map {
          connStr =>
            val endpoint = Endpoint(Seq(connStr))
            val session = new Session(spooky)
            val link = Link.getOrCreate (
              Seq(endpoint),
              factory,
              session
            )
            link
        }
          .collect()

        links1.mkString("\n").shouldBe(
          links2.mkString("\n"),
          sort = true
        )
        assert(spooky.metrics.linkCreated.value == parallelism)
      }
  }
}
