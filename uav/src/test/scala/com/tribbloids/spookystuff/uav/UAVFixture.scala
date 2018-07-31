package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.dsl.{Fleet, Routings, Routing}
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.utils.UAVUtils

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