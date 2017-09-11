package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.dsl.{Fleet, LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.utils.UAVUtils

/**
  * Created by peng on 18/06/17.
  */
trait UAVFixture extends SpookyEnvFixture {

  def simURIs: Seq[String]
  def simUAVs = simURIs.map(v => UAV(Seq(v)))

  //  def parallelism: Int = 3

  def linkFactory: LinkFactory

  override def setUp(): Unit = {
    super.setUp()
    val uavConf = spooky.getConf[UAVConf]
//    uavConf.fastConnectionRetries = 2
    uavConf.fleet = Fleet.Inventory(simUAVs)
    uavConf.linkFactory = linkFactory
    spooky.zeroMetrics()
    UAVUtils.sanityCheck(sc)
  }

  override def tearDown(): Unit = {
    UAVUtils.sanityCheck(sc)
  }
}

trait DummyUAVFixture extends UAVFixture {
  override def linkFactory: LinkFactory = LinkFactories.Dummy

  override lazy val simURIs: Seq[String] = (0 until parallelism).map {
    v =>
      s"dummy:localhost:$v"
  }
}

trait SimUAVFixture extends UAVFixture {
  override lazy val linkFactory: LinkFactory = LinkFactories.ForkToGCS()
}