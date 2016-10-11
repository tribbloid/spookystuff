package com.tribbloids.spookystuff.mav.routing

/**
  * Created by peng on 10/09/16.
  */

object Instance {

  def sitlI2Port(i: Int): Int = {
    val port = 5760 + i * 10
    port
  }

  def getForAPMSITL(n: Int): Seq[Instance] = {
    val is = 0 until n
    is.map {
      i =>
        val port: Int = sitlI2Port(i)
        val endpointTCP = "tcp://localhost:" + port
        Instance(Seq(endpointTCP), None)
    }
  }
  
  def getForPX4SITL(n: Int): Seq[Instance] = {
    ???
  }
}

case class Instance(
                     //remember, one drone can have several telemetry endpoints: 1 primary and several backups (e.g. text message-based)
                     //TODO: implement backup mechanism
                     endpoints: Seq[String],
                     vehicleType: Option[String] = None
                   )

// if out is empty it means an empty node, the most left traversal path of the tree is always the
case class ProxyFactory(
                         //primary localhost out port number -> list of URLs for multicast
                         //the first one used by DK, others nobody cares
                         ports: Seq[Int] = 12014 to 12108,
                         gcsMapping: Map[String, Seq[String]] = Map(),
                         //connection string (RegEx?) pattern => GCS URLs
                         polling: Boolean = false
                       )