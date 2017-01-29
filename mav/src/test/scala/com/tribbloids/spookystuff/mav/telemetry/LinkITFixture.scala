package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.mav.dsl.{LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.mav.sim.SIMFixture
import com.tribbloids.spookystuff.mav.system.Drone
import com.tribbloids.spookystuff.session.Session
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 31/10/16.
  */
object LinkITFixture{

  def testMove(
                spooky: SpookyContext,
                connStrs: Seq[String]
              ): String = {

    val drones = connStrs.map(v => Drone(Seq(v)))
    val session = new Session(spooky)
    val link = Link.trySelect(
      drones,
      session
    )
      .get

    val location = link.Synch.testMove

    location
  }
}

abstract class LinkITFixture extends SIMFixture {

  lazy val linkFactory: LinkFactory = LinkFactories.Direct

  this.spooky.submodule[MAVConf].linkFactory = linkFactory

  //    override def parallelism: Int = 4

  var acc: Int = 0
  def assertLinkCreated(n: Int): Unit ={
    acc += spooky.metrics.linkCreated.value
    assert(acc == n)
  }

  test("move 1 drone") {
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

  test("move drones to different directions") {
    val spooky = this.spooky

    val connStrs = this.simURIs
    val rdd = simURIRDD.map {
      connStr =>
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

  test("move all drones several times") {
    val spooky = this.spooky

    var locations: Array[String] = null
    val connStrs = this.simURIs

    for (i <- 1 to 2) {
      val rdd: RDD[String] = simURIRDD.map {
        connStr =>
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