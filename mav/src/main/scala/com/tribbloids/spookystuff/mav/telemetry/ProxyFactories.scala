package com.tribbloids.spookystuff.mav.telemetry

import scala.util.Random

/**
  * Created by peng on 26/11/16.
  */
object ProxyFactories {

  case object NoProxy extends ProxyFactory {
    def apply(endpoint: Endpoint) = None
  }

  case class ForkToGCS(
                        //primary localhost out port number -> list of URLs for multicast
                        //the first one used by DK, others nobody cares
                        primaryOuts: Seq[String] = (12014 to 12108).map(i => s"udp:localhost:$i"),
                        //this is the default port listened by QGCS
                        gcsMapping: Endpoint => Set[String] = _ => Set("udp:localhost:14550"),
                        //connection string (RegEx?) pattern => GCS URLs
                        polling: Boolean = false
                      ) extends ProxyFactory {

    //CAUTION: DO NOT select primary out sequentially!
    // you can't distinguish vehicle failure and proxy failure, your best shot is to always use a random port for primary out
    def apply(endpoint: Endpoint): Option[Proxy] = {
      ForkToGCS.synchronized {
        val existingPrimaryouts = Proxy.existing.map(identity).map(_.primaryOut)
        val availableOut = primaryOuts.filter {
          v =>
            !existingPrimaryouts.contains(v)
        }

        val primaryOut = availableOut.apply(Random.nextInt(availableOut.size))
        val outs = gcsMapping(endpoint).toSeq
        val result = Proxy(
          endpoint.connStrs.head,
          Seq(primaryOut) ++ outs
        )
        Some(result)
      }
    }

    //    override def unapply(link: Link): Boolean = {
    //      val expectedGCSOuts: Set[String] = gcsMapping(link.endpoint)
    //      link.proxy match {
    //        case None => false
    //        case Some(pp) =>
    //          if (pp.outs.slice(1, Int.MaxValue).toSet == expectedGCSOuts) true
    //          else false
    //      }
    //    }
  }
}
