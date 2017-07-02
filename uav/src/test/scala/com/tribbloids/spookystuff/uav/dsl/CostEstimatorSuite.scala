package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.testutils.FunSpecx
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.spatial.{Location, NED}

/**
  * Created by peng on 7/2/17.
  */
class CostEstimatorSuite extends FunSpecx {

  describe("Default CostEstimator") {

    it("can measure cost of 2 Waypints") {
      val p1 = Location()
      val p2 = Location.fromTuple(NED(3, 4, 0) -> p1)
      val trace = TraceView(List(
        Waypoint(p1),
        Waypoint(p2)
      ))
    }
  }
}
