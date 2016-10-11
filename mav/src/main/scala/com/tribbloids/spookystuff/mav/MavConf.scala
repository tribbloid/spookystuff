package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.AbstractConf
import com.tribbloids.spookystuff.mav.comm.{EndPoint$, ProxyFactory}
import org.apache.spark.SparkConf

object MavConf {


}

/**
  * Created by peng on 04/09/16.
  */
case class MavConf(
                    // list all possible connection string of drones
                    // including tcp, udp and serial,
                    // some of them may be unreachable but you don't care.
                    // connection list is configed by user and shared by all executors
                    // blacklist is node specific and determined by GenPartitioner
                    // routing now becomes part of Connection?
                    var instances: Seq[EndPoint],
                    var proxies: Option[ProxyFactory],
                    var takeOffAltitude: Double = 20 // in meters
                  ) extends AbstractConf {

  override val name: String = "mav"

  // TODO: use reflection to automate, or implement
  override def importFrom(implicit sparkConf: SparkConf): AbstractConf = this
}