package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.conf.{AbstractConf, Submodules}
import com.tribbloids.spookystuff.uav.dsl._
import com.tribbloids.spookystuff.uav.spatial.point.{LLA, Location}
import com.tribbloids.spookystuff.uav.spatial.Anchors
import com.tribbloids.spookystuff.uav.system.UAV
import org.apache.spark.SparkConf

import scala.concurrent.duration._
import scala.util.Random

object UAVConf extends Submodules.Builder[UAVConf]{

  //DO NOT change to val! all confs are mutable
  def default = UAVConf()

  final val DEFAULT_BAUDRATE = 57600
  //  final val DEFAULT_BAUDRATE = 9200 // for testing only

  final val EXECUTOR_SSID = 250
  final val PROXY_SSID = 251
  final val GCS_SSID = 255

  //  final val EARTH_RADIUS = 6378137.0  // Radius of "spherical" earth

  final val FAST_CONNECTION_RETRIES = 2
  final val BLACKLIST_RESET_AFTER = 1.minute

  /**
    * 43.694195,-79.262262,136,353
    */
  final val DEFAULT_HOME_LOCATION: Location = {
    LLA(43.694195, -79.262262, 136) -> Anchors.Geodetic
  }
}

/**
  * Created by peng on 04/09/16.
  */
case class UAVConf(
                    // list all possible connection string of drones
                    // including tcp, udp and serial,
                    // some of them may be unreachable but you don't care.
                    // connection list is configured by user and shared by all executors
                    // blacklist is node specific and determined by GenPartitioner
                    // routing now becomes part of Connection?

                    var fleet: Fleet = Fleet.Inventory(Nil),
                    var linkFactory: LinkFactory = LinkFactories.ForkToGCS(),
                    var fastConnectionRetries: Int = UAVConf.FAST_CONNECTION_RETRIES,

                    //TODO: by convention, should use exponentially increasing intervals
                    var slowConnectionRetries: Int = Int.MaxValue,
                    var slowConnectionRetryInterval: Duration = UAVConf.BLACKLIST_RESET_AFTER, //1 minute

                    var home: Location = UAVConf.DEFAULT_HOME_LOCATION,
                    var costEstimator: CostEstimator = CostEstimator.L2Distance(),

                    var defaultSpeed: Double = 5.0,

                    // used by Rewriters to clamp Waypoint's altitude before being submitted to Global Planner (GenPartitioner)4
                    // IMPORTANT: always rewrite locally first, globally last!
                    // All UAVNavigation will climb into range before doing anything else.
                    var takeoffMinAltitude: Double = 10.0, //TODO: simplify to 10.0 to 121.92?
                    var takeoffMaxAltitude: Double = 121.92

                  ) extends AbstractConf {

  def _home = home.copy(aliasOpt = Some(Anchors.Home))

  def uavsInFleet: Set[UAV] = fleet.apply()

  def uavsInFleetShuffled: Seq[UAV] = Random.shuffle(uavsInFleet.toList)

  // TODO: use reflection to automate
  override def importFrom(sparkConf: SparkConf): UAVConf.this.type = {
    this.copy().asInstanceOf[this.type]
  }
}
