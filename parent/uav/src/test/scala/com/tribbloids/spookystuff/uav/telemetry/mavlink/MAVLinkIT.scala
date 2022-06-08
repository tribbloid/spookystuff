package com.tribbloids.spookystuff.uav.telemetry.mavlink

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.dsl.{Routing, Routings}
import com.tribbloids.spookystuff.uav.sim.{APMQuadFixture, APMSim}
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.{Dispatcher, LinkITFixture}
import com.tribbloids.spookystuff.utils.CommonUtils
import org.scalatest.Ignore

import scala.concurrent.{Await, Future}

/**
  * Created by peng on 27/01/17.
  */
@Ignore
class MAVLinkIT_NoProxy extends MAVLinkIT {

  override lazy val routing = Routings.Direct()
}

class MAVLinkIT extends LinkITFixture with APMQuadFixture {

  override lazy val routing: Routing = Routings.Forked(
    toSparkSize = 2
  )

  it("GCS takeover and relinquish control during flight") {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val spooky = this.spooky

    val rdd = sc.parallelize(fleetURIs).map { connStr =>
      val drones = List(UAV(Seq(connStr)))
      val session = new Session(spooky)
      val link = Dispatcher( //refitting
                            drones,
                            session).get
        .asInstanceOf[MAVLink]

      val endpoint1 = link.Endpoints.primary
      val endpoint2 = link.Endpoints.executors.last
      endpoint1.PY.assureClearanceAlt(20)
      endpoint2.PY.start()

      val moved = Future {
        endpoint1.PY.testMove()
      }
      Thread.sleep(5000 / APMSim.SPEEDUP)

      def changeAndVerifyMode(endpoint: Endpoint, mode: String) = CommonUtils.withDeadline(20.seconds) {
        val py = endpoint.PY
        py.setMode(mode)
        Predef.assert(py.vehicle.mode.name.$STR.get == mode)
      }

      changeAndVerifyMode(endpoint2, "BRAKE")
      //          changeMode(py2, "LOITER")
      Thread.sleep(5000 / APMSim.SPEEDUP)
      changeAndVerifyMode(endpoint2, "GUIDED")
      val position = Await.result(moved, 60.seconds).$STR.get
      println(position)
    }
    val location = rdd.collect().head

    println(location)
    assertMaxLinkCreated(parallelism)
  }
}
