package com.tribbloids.spookystuff.mav.dsl

import com.tribbloids.spookystuff.mav.system.Drone
import com.tribbloids.spookystuff.mav.telemetry.Link
import com.tribbloids.spookystuff.mav.telemetry.mavlink.MAVLink
import com.tribbloids.spookystuff.utils.PrettyProduct

import scala.util.Random

/**
  * Created by peng on 26/11/16.
  */
object LinkFactories {

  case object Direct extends LinkFactory with PrettyProduct{
    def apply(endpoint: Drone) = MAVLink(endpoint)
  }

  case class ForkToGCS(
                        //primary localhost out port number -> list of URLs for multicast
                        //the first one used by DK, others nobody cares
                        toExecutor: Seq[String] = (12014 to 12108).map(i => s"udp:localhost:$i"),
                        //this is the default port listened by QGCS
                        toGCS: Drone => Set[String] = _ => Set("udp:localhost:14550"),
                        ToExecutorSize: Int = 1
                      ) extends LinkFactory with PrettyProduct {

    //CAUTION: DO NOT select primary out sequentially!
    // you can't distinguish vehicle failure and proxy failure, your best shot is to always use a random port for primary out
    def apply(endpoint: Drone): Link = {

      //      LinkFactories.synchronized {
      val existing4Exec: Seq[String] = Link.existing.values.toSeq
        .flatMap {
          case v: MAVLink => v.allURIs
        }
      val available = toExecutor.filter {
        v =>
          !existing4Exec.contains(v)
      }
      val shuffled = Random.shuffle(available)

      val executorOuts = shuffled.slice(0, ToExecutorSize)
      val gcsOuts = toGCS(endpoint).toSeq
      val result = MAVLink(
        endpoint,
        executorOuts,
        gcsOuts
      )
      result
      //      }
    }
  }
}
