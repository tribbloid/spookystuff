package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.uav.actions.UAVPositioning
import com.tribbloids.spookystuff.uav.spatial.LocationGlobal
import org.apache.spark.ml.dsl.utils.FlowUtils

/**
  * Created by peng on 07/02/17.
  */
object PositioningCosts {

  case class Default(
                      defaultSpeed: Double
                    ) extends PositioningCost {

    def apply(trace: Seq[UAVPositioning]): Double = {

      val start_ends = trace.map(_.start_end)
      val allStartEnds: Seq[List[(LocationGlobal, LocationGlobal)]] = FlowUtils.cartesianProductList(start_ends)

      val allCosts = allStartEnds.map {
        list =>
          val actionCosts = list.map {
            tuple =>
//              tuple._1.relativeFrom()
          }
      }
      ???
    }
  }
}
