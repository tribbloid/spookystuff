package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.session.PythonDriver
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.{DummyLink, Link}
import com.tribbloids.spookystuff.uav.telemetry.mavlink.MAVLink

import scala.util.Random

/**
  * Created by peng on 26/11/16.
  */
object Routings {

  case object Dummy extends Routing {
    def apply(uav: UAV) = {
      DummyLink(uav)
    }
  }

  case class Direct(
      override val pythonExe: String = PythonDriver.EXE
  ) extends Routing {

    def apply(uav: UAV): MAVLink = {
      MAVLink(uav)(driverTemplate)
    }
  }

  case class Forked(
      //primary localhost out port number -> list of URLs for multicast
      //the first one used by DK, others nobody cares
      toSpark: Seq[String] = (12014 to 12108).map(i => s"udp:localhost:$i"),
      //this is the default port listened by QGCS
      toGCS: UAV => Set[String] = _ => Set("udp:localhost:14550"),
      toSparkSize: Int = 1,
      override val pythonExe: String = PythonDriver.EXE
  ) extends Routing {

    //CAUTION: DO NOT select primary out sequentially!
    // you can't distinguish vehicle failure and proxy failure, your best shot is to always use a random port for primary out
    def apply(endpoint: UAV): Link = {

      val existing: Seq[String] = Link.registered.values.toSeq
        .flatMap {
          _.resourceURIs
        }
      val available = toSpark.filter { v =>
        !existing.contains(v)
      }
      val shuffled = Random.shuffle(available)

      val executorOuts = shuffled.slice(0, toSparkSize)
      val gcsOuts = toGCS(endpoint).toSeq
      val result = MAVLink(
        endpoint,
        executorOuts,
        gcsOuts
      )(
        driverTemplate
      )
      result
    }
  }
}

abstract class Routing extends Serializable {

  def apply(uav: UAV): Link

  @transient lazy val driverTemplate = new PythonDriver()

  def pythonExe: String = PythonDriver.EXE
}
