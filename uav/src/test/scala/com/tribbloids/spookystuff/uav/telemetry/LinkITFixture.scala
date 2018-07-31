package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.{SITLFixture, UAVMetrics}
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 31/10/16.
  */
object LinkITFixture{

  def testMove(
                spooky: SpookyContext,
                connStrs: List[String]
              ): String = {

    val drones = connStrs.map(v => UAV(Seq(v)))
    val session = new Session(spooky)
    val link = Dispatcher(
      drones,
      session
    )
      .get

    val location = link.synch.testMove

    location
  }
}

abstract class LinkITFixture extends SITLFixture {

  def assertMaxLinkCreated(n: Int): Unit = {
    val acc = spooky.getMetrics[UAVMetrics].linkCreated.value
    assert(acc <= n)
  }

  it("move 1 drone") {
    val spooky = this.spooky

    val rdd = sc.parallelize(Seq(this.fleetURIs.head))
      .map {
        connStr =>
          LinkITFixture.testMove(spooky, List(connStr))
      }
    val location = rdd.collect().head

    println(location)
    assertMaxLinkCreated(1)
  }

  it("move drones to different directions") {
    val spooky = this.spooky

    val connStrs = this.fleetURIs
    val rdd = sc.parallelize(fleetURIs).map {
      _ =>
        LinkITFixture.testMove(spooky, connStrs)
    }
      .persist()
    val locations = rdd.collect()
    assert(locations.distinct.length == locations.length)
    locations.toSeq.foreach(
      println
    )
    assertMaxLinkCreated(parallelism)
  }

  it("move all drones several times") {
    val spooky = this.spooky

    var locations: Array[String] = null
    val connStrs = this.fleetURIs

    for (_ <- 1 to 2) {
      val rdd: RDD[String] = sc.parallelize(fleetURIs).map {
        _ =>
          LinkITFixture.testMove(spooky, connStrs)
      }

      locations = {
        val locations = rdd.collect()
        assert(locations.distinct.length == locations.length)
        locations
      }
    }

    locations.toSeq.foreach(
      println
    )
    assertMaxLinkCreated(parallelism)
  }
}
