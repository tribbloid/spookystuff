package com.tribbloids.spookystuff.mav.routing

/**
  * Created by peng on 10/09/16.
  */

object Instance {

  def getForAPMSITL(n: Int): Seq[Instance] = {
    ???
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
                         gcsMapping: Map[String, Seq[String]] = Map(), //connection URL pattern => GCS URLs
                         polling: Boolean = false
                       )