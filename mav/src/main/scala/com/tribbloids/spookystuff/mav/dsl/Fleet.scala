package com.tribbloids.spookystuff.mav.dsl

import com.tribbloids.spookystuff.mav.system.Drone
import com.tribbloids.spookystuff.utils.SpookyUtils

/**
  * SpookyContext => RDD[Drone]
  */
object Fleet {

  case class Inventory(
                        drones: Seq[Drone],
                        hosts: (Drone, String) => Boolean = (_,_) => true
                      ) extends Fleet {

    def apply(): Seq[Drone] = {
      val hostName = SpookyUtils.getHost_ExecutorID._1
      drones.flatMap {
        d =>
          if (hosts(d, hostName)) Some(d)
          else None
      }
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

    def apply(): Seq[Drone] = {
      val before = delegate()
      ???
    }
  }
}

trait Fleet extends (() => Seq[Drone]) {

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