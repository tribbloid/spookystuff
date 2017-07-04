package com.tribbloids.spookystuff.uav.planning

import com.graphhopper.jsprit.core.util.FastVehicleRoutingTransportCostsMatrix
import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.spatial.{Location, NED}

/**
  * Created by peng on 7/3/17.
  */
class JSpritSolverSuite extends SpookyEnvFixture {

  describe("getCostMatrix") {

    it("can build cost matrix for 3 points") {
      val traces: Seq[TraceView] = Seq(
        List(Waypoint(Location.fromTuple(NED(0,0,0) -> UAVConf.DEFAULT_HOME_LOCATION))),
        List(Waypoint(Location.fromTuple(NED(3,0,0) -> UAVConf.DEFAULT_HOME_LOCATION))),
        List(Waypoint(Location.fromTuple(NED(0,4,0) -> UAVConf.DEFAULT_HOME_LOCATION)))
      )
      val mat: FastVehicleRoutingTransportCostsMatrix = JSpritSolver.getCostMatrix(spooky, traces.zipWithIndex)
      //TODO: add assertion
    }
  }
}
