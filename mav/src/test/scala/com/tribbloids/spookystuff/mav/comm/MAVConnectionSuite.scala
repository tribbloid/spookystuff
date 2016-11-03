package com.tribbloids.spookystuff.mav.comm

import com.tribbloids.spookystuff.mav.APMSimFixture
import com.tribbloids.spookystuff.session.DriverSession

/**
  * Created by peng on 31/10/16.
  */
class MAVConnectionSuite extends APMSimFixture {

  val pf = ProxyFactory()

  test("Can be created per executors and move drones to different directions") {

    val vehicles: Array[String] = runTest(_ => None)

    vehicles.toSeq.foreach(
      println
    )
  }

  ignore("Can be created per executors and move drones to different directions with proxies") {

    val pf = this.pf

    val vehicles: Array[String] = runTest(v => Some(pf.next(v)))

    vehicles.toSeq.foreach(
      println
    )
  }

  def runTest(getProxy: (Endpoint) => Option[Proxy]): Array[String] = {
    val spooky = this.spooky
    val vehicles = simConnStrRDD.map {
      connStr =>
        val endpoint = Endpoint(Seq(connStr))
        val conn = MAVConnection(
          endpoint,
          getProxy(endpoint)
        )
        val session = new DriverSession(spooky)
        val vehicle = conn.sessionPy(session).vehicle
        val result = vehicle.toString
        conn.sessionPy(session).close()
        result
    }
      .collect()
    vehicles
  }
}
