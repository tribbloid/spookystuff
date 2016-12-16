package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.utils.PrettyProduct

import scala.util.Random

/**
  * Created by peng on 26/11/16.
  */
object LinkFactories {

  case object NoProxy extends LinkFactory with PrettyProduct{
    def apply(endpoint: Endpoint) = Link(endpoint, Nil)
  }

  case class ForkToGCS(
                        //primary localhost out port number -> list of URLs for multicast
                        //the first one used by DK, others nobody cares
                        uriCandidates: Seq[String] = (12014 to 12108).map(i => s"udp:localhost:$i"),
                        //this is the default port listened by QGCS
                        gcsMapping: Endpoint => Set[String] = _ => Set("udp:localhost:14550")
                      ) extends LinkFactory with PrettyProduct {

    //CAUTION: DO NOT select primary out sequentially!
    // you can't distinguish vehicle failure and proxy failure, your best shot is to always use a random port for primary out
    def apply(endpoint: Endpoint): Link = {

      LinkFactories.synchronized {
        val existingURIs = Link.existing.values.toSeq.map(_.link.uri)
        val availableURIs = uriCandidates.filter {
          v =>
            !existingURIs.contains(v)
        }

        val primaryOut = availableURIs.apply(Random.nextInt(availableURIs.size))
        val outs = gcsMapping(endpoint).toSeq
        val result = Link(
          endpoint,
          Seq(primaryOut) ++ outs
        )
        result
      }
    }
  }
}
