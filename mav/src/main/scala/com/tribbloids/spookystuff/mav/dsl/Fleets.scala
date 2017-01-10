package com.tribbloids.spookystuff.mav.dsl

import com.tribbloids.spookystuff.mav.telemetry.Drone
import com.tribbloids.spookystuff.utils.SpookyUtils

/**
  * SpookyContext => RDD[Drone]
  */
object Fleets {

  case class Inventory(
                        drones: Seq[Drone],
                        hosts: (Drone, String) => Boolean = (_,_) => true
                      ) extends Fleet {

    def apply(): Seq[(Drone, String)] = {
      val hostName = SpookyUtils.getHost_ExecutorID._1
      drones.flatMap {
        d =>
          if (hosts(d, hostName)) Some(d -> hostName)
          else None
      }
    }
  }

  /**
    * will do a pre-scan to determine the availabiliy of all vehicles
    */
  //TODO: implement later
  case class Discover(delegate: Fleet) extends Fleet {

    def apply(): Seq[(Drone, String)] = {
      val before = delegate()
      ???
    }
  }
}
