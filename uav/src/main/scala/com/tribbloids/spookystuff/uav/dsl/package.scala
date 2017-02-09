package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.uav.actions.UAVPositioning
import com.tribbloids.spookystuff.uav.system.{Base, Drone}
import com.tribbloids.spookystuff.uav.telemetry.Link
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 12/11/16.
  */
package object dsl {

//  type Fleet = () => Seq[Drone] //=> Seq[Drone, hostname={ip:port}]

  type LinkFactory = (Drone => Link)

  type BaseLocator = (RDD[Drone] => Base)

  type PositioningCost = Seq[UAVPositioning] => Double

//  type DronePreference = Drone => Option[Double]
}
