package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.utils.SpookyUtils

/**
  * SpookyContext => RDD[Drone]
  */
object Fleet {

  case class Inventory(
                        uavs: Iterable[UAV],
                        hosts: (UAV, String) => Boolean = (_, _) => true
                      ) extends Fleet {

    def apply(): Set[UAV] = {
      val hostPort = SpookyUtils.blockManagerIDOpt.get.hostPort
      uavs.flatMap {
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
                       delegate: Fleet
//                       base: BaseLocator
                     ) extends Fleet {

    def apply(): Set[UAV] = {
      val before = delegate()
      ???
    }
  }
}

trait Fleet extends (() => Set[UAV]) {

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