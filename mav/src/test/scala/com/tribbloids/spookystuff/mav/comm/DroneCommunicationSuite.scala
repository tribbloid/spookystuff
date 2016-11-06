package com.tribbloids.spookystuff.mav.comm

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.mav.APMSimFixture
import com.tribbloids.spookystuff.session.DriverSession

/**
  * Created by peng on 31/10/16.
  */
object DroneCommunicationSuite{

  val pf = ProxyFactory()

  def testMove1(
                 spooky: SpookyContext,
                 getProxy: (Endpoint) => Option[Proxy],
                 connStr: String
               ): (Pair[DroneCommunication#PyBinding, String]) = {
    val endpoint = Endpoint(Seq(connStr))
    val conn = DroneCommunication(
      endpoint,
      getProxy(endpoint)
    )
    val session = new DriverSession(spooky)

    val location = conn.Py(session)
      .testMove()
        .value

    conn.Py(session) -> location
  }
}

class DroneCommunicationSuite extends APMSimFixture {

  test("Can get connection string from Endpoint") {
    val spooky = this.spooky
    val uris = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val conn = DroneCommunication(
          endpoint,
          None
        )
        val session = new DriverSession(spooky)

        val result = conn.Py(session).uri.valueOpt
        conn.Py(session).close()
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

  test("Can get connection string from Proxy") {
    val spooky = this.spooky
    val conns = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val conn = DroneCommunication(
          endpoint,
          Some(DroneCommunicationSuite.pf.next(endpoint))
        )
        conn
    }
      .persist()
    val outs = conns.map {
      conn =>
        conn.proxy.get.outs
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

    val uris = conns.map {
      conn =>
        val session = new DriverSession(spooky)

        val result = conn.Py(session).uri.valueOpt
        conn.Py(session).close()
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

  test("move 1 drone") {
    val tuple = DroneCommunicationSuite.testMove1(
      this.spooky, v => None,
      this.simConnStrs.head
    )
    println(tuple._2)
    tuple._1.close()
  }

  test("move 1 drone with proxy") {
    val tuple = DroneCommunicationSuite.testMove1(
      this.spooky, v => Some(DroneCommunicationSuite.pf.next(v)),
      this.simConnStrs.head
    )
    println(tuple._2)
    tuple._1.close()
  }

  test("move drones to different directions") {
    val vehicles: Array[String] = testMove(_ => None)

    vehicles.toSeq.foreach(
      println
    )
  }

  test("move drones to different directions with proxies") {
    val vehicles: Array[String] = testMove(v => Some(DroneCommunicationSuite.pf.next(v)))

    vehicles.toSeq.foreach(
      println
    )
  }

  def testMove(getProxy: (Endpoint) => Option[Proxy]): Array[String] = {
    val spooky = this.spooky
    val rdd = simConnStrRDD.map {
      connStr =>
        val tuple = DroneCommunicationSuite.testMove1(spooky, getProxy, connStr)

        tuple._1.close()
        tuple
    }
      .persist()
    try {
      val locations = rdd.map(_._2).collect()
      assert(locations.distinct.length == locations.length)
      locations
    }
  }
}
