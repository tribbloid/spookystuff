package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.mav.dsl.{LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.mav.sim.{APMSim, APMSimFixture}
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

    val endpoints = connStrs.map(v => Drone(Seq(v)))
    val session = new Session(spooky)
    val link = Link.getOrInitialize(
      endpoints,
      factory,
      session
    )

    val location = link.primaryEndpoint.Py(session)
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

  override lazy val factory = LinkFactories.ForkToGCS(
    ToExecutorSize = 2
  )

  test("GCS takeover and relinquish control during flight") {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val spooky = this.spooky
    val factory = this.factory

    val rdd = simConnStrRDD.map {
        connStr =>
          val drones = Seq(Drone(Seq(connStr)))
          val session = new Session(spooky)
          val link = Link.getOrInitialize( //refitting
            drones,
            factory,
            session
          )

          val endpoint1 = link.primaryEndpoint
          val endpoint2 = link.endpointsForExecutor.last
          endpoint1.Py(session).assureClearanceAlt(20)
          val driver2 = new PythonDriver()
          val py2 = endpoint2._Py(driver2, Some(spooky))
          py2.start()

          val moved = Future {
            endpoint1.Py(session).testMove()
          }
          Thread.sleep(5000 / APMSim.SPEEDUP)

          def changeMode(py: Endpoint#Binding, mode: String) = {
            py.mode(mode)
            TestHelper.assert(py.vehicle.mode.name.$STR.get == mode)
          }

          changeMode(py2, "BRAKE")
          changeMode(py2, "LOITER")
          Thread.sleep(5000 / APMSim.SPEEDUP)
          py2.mode("GUIDED")
          val position = Await.result(moved, 60.seconds).$STR.get
          println(position)
      }
    val location = rdd.collect().head

    println(location)
    assert(spooky.metrics.linkCreated.value == 0)
  }
}
