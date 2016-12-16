package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.AbstractConf
import com.tribbloids.spookystuff.mav.telemetry.{Endpoint, LinkFactories, LinkFactory}
import org.apache.spark.SparkConf

object MAVConf {

  def default() = MAVConf()
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
                    var takeOffAltitude: Double = 20 // in meters
                  ) extends AbstractConf {

  // TODO: use reflection to automate, or implement
  override def importFrom(implicit sparkConf: SparkConf): this.type = this
}