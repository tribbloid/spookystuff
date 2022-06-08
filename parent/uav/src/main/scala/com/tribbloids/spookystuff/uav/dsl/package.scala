package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.uav.system.{Base, UAV}
import com.tribbloids.spookystuff.uav.telemetry.Link
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 12/11/16.
  */
package object dsl {

  //  type Fleet = () => Seq[Drone] //=> Seq[Drone, hostname={ip:port}]

  //  type LinkFactory = (UAV => Link)

  //  type BaseLocator = (RDD[UAV] => Base)

  //  type DronePreference = Drone => Option[Double]
}
