package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.mav.dsl.{LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.mav.sim.APMSimFixture
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.testutils.TestHelper
import org.apache.spark.rdd.RDD

import scala.concurrent.{Await, Future}

/**
  * Created by peng on 31/10/16.
  */
object LinkIT{

  def moveAndGetLocation(
                          spooky: SpookyContext,
                          factory: LinkFactory,
                          connStrs: Seq[String]
                        ): String = {

    val endpoints = connStrs.map(v => Endpoint(Seq(v)))
    val session = new Session(spooky)
    val link = Link.getOrInitialize(
      endpoints,
      factory,
      session
    )

    val location = link.primary.Py(session)
      .testMove()
      .$STR
      .get

    location
  }
}

class LinkIT extends APMSimFixture {

  lazy val factory: LinkFactory = LinkFactories.NoProxy

//    override def parallelism: Int = 4

  test("move 1 drone") {
    val spooky = this.spooky
    val factory = this.factory
    val rdd = sc.parallelize(Seq(this.simConnStrs.head))
      .map {
        connStr =>
          LinkIT.moveAndGetLocation(spooky,
            factory, Seq(connStr))
      }
    val location = rdd.collect().head

    println(location)
    assert(spooky.metrics.linkCreated.value == 1)
  }

  test("move drones to different directions") {
    val spooky = this.spooky
    val factory = this.factory
    val connStrs = this.simConnStrs
    val rdd = simConnStrRDD.map {
      connStr =>
        LinkIT.moveAndGetLocation(spooky, factory, connStrs)
    }
      .persist()
    val locations = rdd.collect()
    assert(locations.distinct.length == locations.length)
    locations.toSeq.foreach(
      println
    )
    assert(spooky.metrics.linkCreated.value == parallelism - 1)
  }

  test("move all drones several times") {
    val spooky = this.spooky
    val factory = this.factory
    var locations: Array[String] = null
    val connStrs = this.simConnStrs

    for (i <- 1 to 2) {
      val rdd: RDD[String] = simConnStrRDD.map {
        connStr =>
          LinkIT.moveAndGetLocation(spooky, factory, connStrs)
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
    assert(spooky.metrics.linkCreated.value == 0)
  }
}

class LinkWithProxyIT extends LinkIT {

  override lazy val factory = LinkFactories.ForkToGCS()

//  test("GCS takeover and relinquish control during flight") {
//    import scala.concurrent.duration._
//    import scala.concurrent.ExecutionContext.Implicits.global
//
//    val spooky = this.spooky
//    val rdd = simConnStrRDD.map {
//        connStr =>
//          val endpoints = Seq(Endpoint(Seq(connStr)))
//          val factory = LinkFactories.ForkToGCS(
//            getGCSOuts = _ =>
//              Set("udp:localhost:14550", "udp:localhost:14560")
//          )
//          val session = new Session(spooky)
//          val link = Link.getOrInitialize( //refitting
//            endpoints,
//            factory,
//            session
//          )
//          link.Py(session).assureClearanceAlt(20)
//
//          val newDriver = new PythonDriver()
//          val sublink = link.sublink(2, 252)
//          val subPy = sublink._Py(newDriver, Some(spooky))
//
//          val moved = Future {
//            link.Py(session).testMove()
//          }
//          Thread.sleep(5000)
//          println("BRAKE!!!!!!!!!!!!!!!")
//          subPy.mode("BRAKE")
//          TestHelper.assert(subPy.vehicle.mode.name.$STR.get == "BRAKE")
//          subPy.mode("ALT_HOLD")
//          TestHelper.assert(subPy.vehicle.mode.name.$STR.get == "ALT_HOLD")
//          Thread.sleep(10000)
//          subPy.mode("GUIDED")
//          val position = Await.result(moved, 60.seconds).$STR.get
//          println(position)
//      }
//    val location = rdd.collect().head
//
//    println(location)
//    assert(spooky.metrics.linkCreated.value == 0)
//  }
}
