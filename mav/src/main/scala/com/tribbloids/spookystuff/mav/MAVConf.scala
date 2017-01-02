package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.AbstractConf
import com.tribbloids.spookystuff.mav.dsl.{LinkFactories, LinkFactory}
import com.tribbloids.spookystuff.mav.telemetry.Endpoint
import org.apache.spark.SparkConf

object MAVConf {

  def default() = MAVConf()

  final val DEFAULT_BAUDRATE = 57600
//  final val DEFAULT_BAUDRATE = 9200 // for testing only

  final val LINK_SSID = 250
  final val PROXY_SSID = 251

  final val EARTH_RADIUS = 6378137.0  // Radius of "spherical" earth
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
                    var endpoints: Seq[Endpoint] = Nil,
                    var proxyFactory: LinkFactory = LinkFactories.ForkToGCS(),
                    var connectionRetries: Int = 3,
                    var clearanceAltitude: Double = 20 // in meters
                  ) extends AbstractConf {

  // TODO: use reflection to automate, or implement
  override def importFrom(implicit sparkConf: SparkConf): this.type = this
}