package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVFixture
import com.tribbloids.spookystuff.uav.dsl.{LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.uav.system.UAV
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 31/10/16.
  */
object LinkITFixture{

  def testMove(
                spooky: SpookyContext,
                connStrs: Seq[String]
              ): String = {

    val drones = connStrs.map(v => UAV(Seq(v)))
    val session = new Session(spooky)
    val link = Link.trySelect(
      drones,
      session
    )
      .get

    val location = link.synch.testMove

    location
  }
}

abstract class LinkITFixture extends UAVFixture {

  override lazy val linkFactory: LinkFactory = LinkFactories.Direct

  var acc: Int = 0
  def assertLinkCreated(n: Int): Unit ={
    acc += spooky.spookyMetrics.linkCreated.value
    assert(acc == n)
  }

  it("move 1 drone") {
    val spooky = this.spooky

    val rdd = sc.parallelize(Seq(this.simURIs.head))
      .map {
        connStr =>
          LinkITFixture.testMove(spooky, Seq(connStr))
      }
    val location = rdd.collect().head

    println(location)
    assertLinkCreated(1)
  }

  it("move drones to different directions") {
    val spooky = this.spooky

    val connStrs = this.simURIs
    val rdd = simURIRDD.map {
      _ =>
        LinkITFixture.testMove(spooky, connStrs)
    }
      .persist()
    val locations = rdd.collect()
    assert(locations.distinct.length == locations.length)
    locations.toSeq.foreach(
      println
    )
    assertLinkCreated(parallelism)
  }

  it("move all drones several times") {
    val spooky = this.spooky

    var locations: Array[String] = null
    val connStrs = this.simURIs

    for (_ <- 1 to 2) {
      val rdd: RDD[String] = simURIRDD.map {
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
    assertLinkCreated(parallelism)
  }
}