package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.spatial.point.{Location, NED}
import org.scalactic.TolerantNumerics

/**
  * Created by peng on 7/2/17.
  */
class CostEstimatorSuite extends SpookyEnvFixture {

  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1e-4)

  describe("Default CostEstimator") {

    it("can measure cost of 2 Waypints") {
      val p1 = Location(Nil)
      val p2 = Location.fromTuple(NED(3, 4, 0) -> p1)
      val trace = List(
        Waypoint(p1),
        Waypoint(p2)
      )
      val estimator = CostEstimator.L2Distance()
      assert(estimator.estimate(trace, defaultSchema) == 5.0)
      //      assert(estimator.estimate(trace.reverse, spooky) == 5.0)
    }

    it("can measure cost of 2 Waypints 2") {
      val p1 = Location.fromTuple(NED(0, 0, 0) -> UAVConf.DEFAULT_HOME_LOCATION)
      val p2 = Location.fromTuple(NED(3, 4, 0) -> UAVConf.DEFAULT_HOME_LOCATION)
      val trace = List(
        Waypoint(p1),
        Waypoint(p2)
      )
      val estimator = CostEstimator.L2Distance()
      assert(estimator.estimate(trace, defaultSchema) === 5.0)
      assert(estimator.estimate(trace.reverse, defaultSchema) === 5.0)
    }

    it("can measure cost of 2 Waypints 3") {
      val p1 = Location.fromTuple(NED(3, 0, 0) -> UAVConf.DEFAULT_HOME_LOCATION)
      val p2 = Location.fromTuple(NED(0, 4, 0) -> UAVConf.DEFAULT_HOME_LOCATION)
      val trace = List(
        Waypoint(p1),
        Waypoint(p2)
      )
      val estimator = CostEstimator.L2Distance()
      assert(estimator.estimate(trace, defaultSchema) === 5.0)
      assert(estimator.estimate(trace.reverse, defaultSchema) === 5.0)
    }

    it("can measure cost of 2 Waypints 4") {
      val p1 = Location.fromTuple(NED(0, 0, 0) -> UAVConf.DEFAULT_HOME_LOCATION)
      val p2 = Location.fromTuple(NED(5, 0, 0) -> UAVConf.DEFAULT_HOME_LOCATION)
      val trace = List(
        Waypoint(p1),
        Waypoint(p2)
      )
      val estimator = CostEstimator.L2Distance()
      assert(estimator.estimate(trace, defaultSchema) === 5.0)
      assert(estimator.estimate(trace.reverse, defaultSchema) === 5.0)
    }
  }
}
