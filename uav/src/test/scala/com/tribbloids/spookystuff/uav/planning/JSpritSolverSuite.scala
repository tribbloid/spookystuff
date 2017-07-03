package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.spatial.{GeodeticAnchor, Location, NED}

/**
  * Created by peng on 7/3/17.
  */
class JSpritSolverSuite extends SpookyEnvFixture {

  describe("getCostMatrix") {

    it("can build cost matrix for 3 points") {
      val traces = Seq(
        List(Waypoint(Location.fromTuple(NED(0,0,0) -> GeodeticAnchor))),
        List(Waypoint(Location.fromTuple(NED(3,0,0) -> GeodeticAnchor))),
        List(Waypoint(Location.fromTuple(NED(0,4,0) -> GeodeticAnchor)))
      )
      val mat = JSpritSolver.getCostMatrix(spooky, )
    }
  }
}
