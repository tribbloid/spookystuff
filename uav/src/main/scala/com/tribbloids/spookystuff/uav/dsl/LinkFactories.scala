package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.uav.telemetry.mavlink.MAVLink
import com.tribbloids.spookystuff.utils.PrettyProduct

import scala.util.Random

/**
  * Created by peng on 26/11/16.
  */
object LinkFactories {

  abstract class MAVLinkFactory extends LinkFactory {

  }

  case object Direct extends MAVLinkFactory {
    def apply(uav: UAV) = MAVLink(uav)
  }

  case class ForkToGCS(
                        //primary localhost out port number -> list of URLs for multicast
                        //the first one used by DK, others nobody cares
                        toSparkNode: Seq[String] = (12014 to 12108).map(i => s"udp:localhost:$i"),
                        //this is the default port listened by QGCS
                        toGCS: UAV => Set[String] = _ => Set("udp:localhost:14550"),
                        ToExecutorSize: Int = 1
                      ) extends MAVLinkFactory {

    //CAUTION: DO NOT select primary out sequentially!
    // you can't distinguish vehicle failure and proxy failure, your best shot is to always use a random port for primary out
    def apply(endpoint: UAV): Link = {

      //      LinkFactories.synchronized {
      val existing4Exec: Seq[String] = Link.existing.values.toSeq
        .flatMap {
          case v: MAVLink => v.allURIs
        }
      val available = toSparkNode.filter {
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

abstract class LinkFactory {

  def apply(uav: UAV): Link

  //    def canCreate()
}