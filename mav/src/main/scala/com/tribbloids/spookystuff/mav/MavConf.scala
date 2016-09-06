package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.AbstractConf
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
                    var connections: Seq[Connection],
                    var routing: Option[Routing],
                    var takeOffAltitude: Double = 20 // in meters
                  ) extends AbstractConf {

  override val name: String = "mav"

  // TODO: use reflection to automate, or implement
  override def importFrom(implicit sparkConf: SparkConf): AbstractConf = this
}

object Connection {

  def getForAPMSITL(n: Int): Seq[Connection] = {
    ???
  }

  def getForPX4SITL(n: Int): Seq[Connection] = {
    ???
  }
}

case class Connection(
                       vehicleURL: String,
                       vehicleType: Option[String] = None
                     )

// if out is empty it means an empty node, the most left traversal path of the tree is always the
case class Routing(
                    //primary localhost out port number -> list of URLs for multicast
                    //the first one used by DK, others nobody cares
                    ports: Seq[Int] = 12014 to 12108,
                    gcsMapping: Map[String, Seq[String]] = Map(), //connection URL pattern => GCS URLs
                    polling: Boolean = false
                  )