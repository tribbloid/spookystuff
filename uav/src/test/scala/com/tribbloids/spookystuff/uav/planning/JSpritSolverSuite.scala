package com.tribbloids.spookystuff.uav.planning

import com.graphhopper.jsprit.core.problem.VehicleRoutingProblem
import com.graphhopper.jsprit.core.util.{FastVehicleRoutingTransportCostsMatrix, VehicleRoutingTransportCostsMatrix}
import com.graphhopper.jsprit.io.problem.VrpXMLReader
import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.dsl.GenPartitioners
import com.tribbloids.spookystuff.uav.spatial.{Location, NED}
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.LinkStatus

/**
  * Created by peng on 7/3/17.
  */
trait JSpritFixture extends SpookyEnvFixture {

  var i = 1

  def getJSprit: GenPartitioners.JSprit = {
    val solutionPath = s"log/JSprit/${this.getClass.getSimpleName}.$i.solution.png"
    val progressPath = s"log/JSprit/${this.getClass.getSimpleName}.$i.progress.png"
    i += 1
    GenPartitioners.JSprit(
      numUAVsOpt = Some(this.parallelism),
      solutionPlotPathOpt = Some(solutionPath),
      progressPlotPathOpt = Some(progressPath)
    )
  }
}

class JSpritSolverSuite extends JSpritFixture {

  val waypoints: Array[TraceView] = Array(
    List(Waypoint(Location.fromTuple(NED(3,4,0) -> UAVConf.DEFAULT_HOME_LOCATION))),
    List(Waypoint(Location.fromTuple(NED(3,0,0) -> UAVConf.DEFAULT_HOME_LOCATION))),
    List(Waypoint(Location.fromTuple(NED(0,4,0) -> UAVConf.DEFAULT_HOME_LOCATION)))
  )

  it("getCostMatrix") {
    val mat: FastVehicleRoutingTransportCostsMatrix = JSpritSolver
      .getCostMatrix(spooky, waypoints.zipWithIndex)
    //TODO: add assertion

    val mat2 = for(
      i <- mat.getMatrix.toList.zipWithIndex;
      j <- i._1.toList.zipWithIndex
    ) yield {
      (i._2, j._2, j._1.toList.map(v => (v * 1000.0).toInt))
    }

    mat2.mkString("\n").shouldBe(
      """
        |(0,0,List(0, 0))
        |(0,1,List(4000, 4000))
        |(0,2,List(3000, 3000))
        |(1,0,List(4000, 4000))
        |(1,1,List(0, 0))
        |(1,2,List(5000, 5000))
        |(2,0,List(3000, 3000))
        |(2,1,List(5000, 5000))
        |(2,2,List(0, 0))
      """.stripMargin
    )
  }

  describe("objectiveFunction") {

    it("can evaluate 1 route") {
      val location = UAVConf.DEFAULT_HOME_LOCATION
      val uav = LinkStatus(UAV(Seq("dummy@localhost")), None, location, location)
      val solver = JSpritSolver[Int](getJSprit, spooky, Array(uav), waypoints.map(v => v -> Nil))

      val solution = solver.solve

      val cost = JSpritSolver.objectiveFunction.getCosts(solution)
      assert((cost * 1000).toInt == 10000)

      val map = solver.getUAV2RowsMap

      val first = map.head
      val trace = List(WaypointPlaceholder(first._1.currentLocation)) ++
        first._2.flatMap(_._1.children)

      val cost2 = spooky.getConf[UAVConf].costEstimator.estimate(trace, spooky)
      assert(cost == cost2)
    }
  }

  it("solveVRP") {

    val vrpBuilder = VehicleRoutingProblem.Builder.newInstance
    new VrpXMLReader(vrpBuilder).read("input/abe/abrahamProblem.xml")
    val matrixBuilder = VehicleRoutingTransportCostsMatrix.Builder.newInstance(true)
    val matrixReader = new MatrixReader(matrixBuilder)
    matrixReader.read("input/abe/Matrix.txt")
    val matrix = matrixBuilder.build
    vrpBuilder.setRoutingCost(matrix)

    val vrp = vrpBuilder.build

    val tuple = JSpritSolver.solveVRP(vrp, getJSprit)

    System.out.println("cost: " + tuple._2)
    assert(tuple._2 <= 1011.777)
  }
}
