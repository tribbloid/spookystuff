package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.AbstractConf
import com.tribbloids.spookystuff.mav.actions.LocationGlobal
import com.tribbloids.spookystuff.mav.dsl._
import com.tribbloids.spookystuff.mav.system.Drone
import com.tribbloids.spookystuff.mav.sim.APMSim
import org.apache.spark.SparkConf

object MAVConf {

  //DO NOT change to val! all confs are mutable
  def default = MAVConf()

  final val DEFAULT_BAUDRATE = 57600
  //  final val DEFAULT_BAUDRATE = 9200 // for testing only

  final val EXECUTOR_SSID = 250
  final val PROXY_SSID = 251
  final val GCS_SSID = 255

  final val EARTH_RADIUS = 6378137.0  // Radius of "spherical" earth

  final val CONNECT_RETRIES = 2
  final val BLACKLIST_RESET = 60*1000
}

/**
  * Created by peng on 04/09/16.
  */
case class MAVConf(
                    // list all possible connection string of drones
                    // including tcp, udp and serial,
                    // some of them may be unreachable but you don't care.
                    // connection list is configed by user and shared by all executors
                    // blacklist is node specific and determined by GenPartitioner
                    // routing now becomes part of Connection?
                    var fleet: Fleet = Fleet.Inventory(Nil),
                    var linkFactory: LinkFactory = LinkFactories.ForkToGCS(),
                    var connectRetries: Int = MAVConf.CONNECT_RETRIES,
                    var blacklistReset: Long = MAVConf.BLACKLIST_RESET, //1 minute
                    var clearanceAltitude: Double = 10, // in meters
                    var locationReference: LocationGlobal = APMSim.HOME // reference used to convert LocationLocal to LocationGlobal
                  ) extends AbstractConf {

  /**
    * singleton per worker, lost on shipping
    */
  def drones: Seq[Drone] = fleet.apply()

  // TODO: use reflection to automate
  override def importFrom(implicit sparkConf: SparkConf): MAVConf.this.type = this.copy().asInstanceOf[this.type]
}