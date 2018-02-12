package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.{DummyLink, Link}
import com.tribbloids.spookystuff.uav.telemetry.mavlink.MAVLink

import scala.util.Random

/**
  * Created by peng on 26/11/16.
  */
object LinkFactories {

  case object Dummy extends LinkFactory {
    def apply(uav: UAV) = DummyLink(uav)
  }

  case class Direct(
                     override val pythonExe: String = "python2"
                   ) extends LinkFactory {

    def apply(uav: UAV) = MAVLink(uav, driverTemplate = driverTemplate)
  }

  case class ForkToGCS(
                        //primary localhost out port number -> list of URLs for multicast
                        //the first one used by DK, others nobody cares
                        toSpark: Seq[String] = (12014 to 12108).map(i => s"udp:localhost:$i"),
                        //this is the default port listened by QGCS
                        toGCS: UAV => Set[String] = _ => Set("udp:localhost:14550"),
                        toSparkSize: Int = 1,
                        override val pythonExe: String = "python2"
                      ) extends LinkFactory {

    //CAUTION: DO NOT select primary out sequentially!
    // you can't distinguish vehicle failure and proxy failure, your best shot is to always use a random port for primary out
    def apply(endpoint: UAV): Link = {

      val existing: Seq[String] = Link.registered.values.toSeq
        .flatMap {
          _.exclusiveURIs
        }
      val available = toSpark.filter {
        v =>
          !existing.contains(v)
      }
      val shuffled = Random.shuffle(available)

      val executorOuts = shuffled.slice(0, toSparkSize)
      val gcsOuts = toGCS(endpoint).toSeq
      val result = MAVLink(
        endpoint,
        executorOuts,
        gcsOuts,
        driverTemplate = driverTemplate
      )
      result
    }
  }
}

abstract class LinkFactory extends Serializable {

  def apply(uav: UAV): Link

  def pythonExe: String = "python2"

  @transient lazy val driverTemplate = new PythonDriver(pythonExe)
}