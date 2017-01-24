package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.mav.system.{Base, Drone}
import com.tribbloids.spookystuff.mav.telemetry.Link
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 12/11/16.
  */
package object dsl {

//  type Fleet = () => Seq[Drone] //=> Seq[Drone, hostname={ip:port}]

  type LinkFactory = (Drone => Link)

  type BaseLocator = (RDD[Drone] => Base)

//  type DronePreference = Drone => Option[Double]
}
