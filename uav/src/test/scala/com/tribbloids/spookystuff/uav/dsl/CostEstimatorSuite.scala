package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.spatial.{Location, NED}

/**
  * Created by peng on 7/2/17.
  */
class CostEstimatorSuite extends SpookyEnvFixture {

  describe("Default CostEstimator") {

    it("can measure cost of 2 Waypints") {
      val p1 = Location()
      val p2 = Location.fromTuple(NED(3, 4, 0) -> p1)
      val trace = List(
        Waypoint(p1),
        Waypoint(p2)
      )
      val estimator = CostEstimator.Default()
      assert(estimator.estimate(trace, spooky) == 5.0)
//      assert(estimator.estimate(trace.reverse, spooky) == 5.0)
    }
  }
}
