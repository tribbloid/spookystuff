package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.uav.system.Drone
import com.tribbloids.spookystuff.utils.SpookyUtils

/**
  * SpookyContext => RDD[Drone]
  */
object Fleet {

  case class Inventory(
                        drones: Iterable[Drone],
                        hosts: (Drone, String) => Boolean = (_,_) => true
                      ) extends Fleet {

    def apply(): Set[Drone] = {
      val hostPort = SpookyUtils.getBlockManagerID.hostPort
      drones.flatMap {
        d =>
          if (hosts(d, hostPort)) Some(d)
          else None
      }
        .toSet
    }
  }

  /**
    * will do a pre-scan to determine the availabiliy of all vehicles
    */
  //TODO: implement later
  case class Discover(
                       delegate: Fleet,
                       base: BaseLocator
                     ) extends Fleet {

    def apply(): Set[Drone] = {
      val before = delegate()
      ???
    }
  }
}

trait Fleet extends (() => Set[Drone]) {

//  import com.tribbloids.spookystuff.utils.SpookyViews._
//
//  def base: BaseLocation
//
//  def getBase(spooky: SpookyContext): LocationGlobal = {
//
//    val droneRDD: RDD[Drone] = spooky.sparkContext.mapPerWorker {
//      val drones = this.apply()
//      drones.foreach {
//        d =>
//          Try {
//            d.updateStatus(spooky)
//          }
//      }
//      drones
//    }
//      .flatMap(identity)
//
//    base.apply(droneRDD)
//  }
}