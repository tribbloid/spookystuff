package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.dsl.{Fleet, LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.uav.system.UAV
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 18/06/17.
  */
trait UAVFixture extends SpookyEnvFixture {

  def simURIs: Seq[String]
  def simUAVs = simURIs.map(v => UAV(Seq(v)))

  def parallelism: Int = sc.defaultParallelism
  //  def parallelism: Int = 3

  lazy val linkFactory: LinkFactory = LinkFactories.Dummy

  override def setUp(): Unit = {
    super.setUp()
    val uavConf = spooky.getConf[UAVConf]
//    uavConf.fastConnectionRetries = 2
    uavConf.fleet = Fleet.Inventory(simUAVs)
    uavConf.linkFactory = linkFactory
  }
}
