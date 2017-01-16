package com.tribbloids.spookystuff.mav.dsl
import com.tribbloids.spookystuff.mav.actions.LocationGlobal
import com.tribbloids.spookystuff.mav.hardware.Drone
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 15/01/17.
  */
object BaseLocation {

  object FirstInFleet extends BaseLocation {

    override def apply(v1: RDD[Drone]): LocationGlobal = {
      v1.flatMap {
        _.home
      }
        .first()
    }
  }

  //TODO should be home of all UNARMED vehicles: they can be moving on a truck.
  //See http://ardupilot.org/plane/docs/starting-up-and-calibrating-arduplane.html#setting-the-home-position
}
