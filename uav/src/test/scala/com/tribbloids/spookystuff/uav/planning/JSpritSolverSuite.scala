package com.tribbloids.spookystuff.uav.planning

import com.graphhopper.jsprit.analysis.toolbox.Plotter
import com.graphhopper.jsprit.core.problem.VehicleRoutingProblem
import com.graphhopper.jsprit.core.util.{FastVehicleRoutingTransportCostsMatrix, VehicleRoutingTransportCostsMatrix}
import com.graphhopper.jsprit.io.problem.VrpXMLReader
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

  it("1.1 solveVRP can get minimax solution") {

    val vrpBuilder = VehicleRoutingProblem.Builder.newInstance
    new VrpXMLReader(vrpBuilder).read("input/abe/abrahamProblem.xml")
    val matrixBuilder = VehicleRoutingTransportCostsMatrix.Builder.newInstance(true)
    val matrixReader = new MatrixReader(matrixBuilder)
    matrixReader.read("input/abe/Matrix.txt")
    val matrix = matrixBuilder.build
    vrpBuilder.setRoutingCost(matrix)

    val vrp = vrpBuilder.build

    val tuple = JSpritSolver.solveVRP(vrp)

    System.out.println("cost: " + tuple._2)
    assert(tuple._2 <= 1773.274)
  }
}
