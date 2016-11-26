package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.mav.APMSimFixture
import com.tribbloids.spookystuff.session.DriverSession

/**
  * Created by peng on 31/10/16.
  */
object LinkIT{

  def testMove1(
                 spooky: SpookyContext,
                 proxyFactory: ProxyFactory,
                 connStr: String
               ): (Pair[Link#PyBinding, String]) = {
    val endpoint = Endpoint(Seq(connStr))
    val session = new DriverSession(spooky)
    val link = Link.getOrCreate(
      Seq(endpoint),
      proxyFactory,
      session.getOrProvisionPythonDriver
    )

    val location = link.Py(session)
      .testMove()
      .strOpt.get

    link.Py(session) -> location
  }
}

class LinkIT extends APMSimFixture {

  val proxyFactory = ProxyFactories.ForkToGCS()

  test("move 1 drone") {
    val tuple = LinkIT.testMove1(
      this.spooky, ProxyFactories.NoProxy,
      this.simConnStrs.head
    )
    println(tuple._2)
    tuple._1.close()
  }

  test("move drones to different directions") {
    val vehicles: Array[String] = testMove(ProxyFactories.NoProxy)

    vehicles.toSeq.foreach(
      println
    )
  }

  test("move 1 drone with proxy") {
    val tuple = LinkIT.testMove1(
      this.spooky,
      this.proxyFactory,
      this.simConnStrs.head
    )
    println(tuple._2)
    tuple._1.close()
  }

  test("move drones to different directions with proxies") {
    val vehicles: Array[String] = testMove(this.proxyFactory)

    vehicles.toSeq.foreach(
      println
    )
  }

  def testMove(getProxy: ProxyFactory): Array[String] = {
    val spooky = this.spooky
    val rdd = simConnStrRDD.map {
      connStr =>
        val tuple = LinkIT.testMove1(spooky, getProxy, connStr)

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
