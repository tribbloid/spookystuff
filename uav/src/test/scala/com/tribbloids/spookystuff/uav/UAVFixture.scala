package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.dsl.{Fleet, Routing, Routings}
import com.tribbloids.spookystuff.uav.sim.APMSim
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.uav.utils.UAVUtils
import com.tribbloids.spookystuff.utils.lifespan.Cleanable
import org.apache.spark.SparkContext

object UAVFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def cleanSweepLocally() = {
    Cleanable.cleanSweepAll {
      case _: Link => true
      case _: APMSim => true
      case _ => false
    }
  }


  def cleanSweep(sc: SparkContext) = {
    sc.runEverywhere() {
      _ =>
        // in production Links will be cleaned up by shutdown hook

        cleanSweepLocally()
        Predef.assert(
          Link.registered.isEmpty,
          Link.registered.keys.mkString("\n")
        )
    }

    val isEmpty = sc.runEverywhere() {_ => APMSim.existing.isEmpty}
    assert(!isEmpty.contains(false))
  }
}

/**
  * Created by peng on 18/06/17.
  */
trait UAVFixture extends SpookyEnvFixture {

  {
    UAVConf
    UAVMetrics
  }

  def fleetURIs: Seq[String]
  def fleet = fleetURIs.map(v => UAV(Seq(v)))

  //  def parallelism: Int = 3

  def routing: Routing

  override def setUp(): Unit = {
    super.setUp()
    val uavConf = spooky.getConf[UAVConf]
//    uavConf.fastConnectionRetries = 2
    uavConf.fleet = Fleet.Inventory(fleet)
    uavConf.routing = routing
    spooky.zeroMetrics()
    UAVUtils.sanityCheck(sc)
  }

  override def tearDown(): Unit = {
    UAVUtils.sanityCheck(sc)
  }


  override def beforeAll(): Unit = {

    UAVFixture.cleanSweep(sc)
    Thread.sleep(2000)

    super.beforeAll()
    // small delay added to ensure that cleanSweep
    // won't accidentally clean object created in the suite
  }

  override def afterAll(): Unit = {
    UAVFixture.cleanSweep(sc)
    super.afterAll()
  }
}

trait DummyUAVFixture extends UAVFixture {
  override def routing: Routing = Routings.Dummy

  override  val fleetURIs: Seq[String] = (0 until parallelism).map {
    v =>      s"dummy:localhost:$v"
  }
}

trait SITLFixture extends UAVFixture {
  override lazy val routing: Routing = Routings.Forked()
}