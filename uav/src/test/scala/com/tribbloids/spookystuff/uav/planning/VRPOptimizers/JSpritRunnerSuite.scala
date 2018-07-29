package com.tribbloids.spookystuff.uav.planning.VRPOptimizers

import com.graphhopper.jsprit.core.problem.VehicleRoutingProblem
import com.graphhopper.jsprit.core.util.{FastVehicleRoutingTransportCostsMatrix, VehicleRoutingTransportCostsMatrix}
import com.graphhopper.jsprit.io.problem.VrpXMLReader
import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.spatial.point.{Location, NED}
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.telemetry.LinkStatus
import com.tribbloids.spookystuff.uav.utils.Lock

class JSpritRunnerSuite extends VRPFixture {

  val waypoints: Array[TraceView] = Array[TraceView](
    List(Waypoint(NED(3, 4, 0) -> UAVConf.DEFAULT_HOME_LOCATION: Location)),
    List(Waypoint(NED(3, 0, 0) -> UAVConf.DEFAULT_HOME_LOCATION: Location)),
    List(Waypoint(NED(0, 4, 0) -> UAVConf.DEFAULT_HOME_LOCATION: Location))
  )

  it("getCostMatrix") {
    val mat: FastVehicleRoutingTransportCostsMatrix = JSpritRunner
      .getCostMatrix(defaultSchema, waypoints.zipWithIndex)
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
      val uav = LinkStatus(UAV(Seq("dummy@localhost")), Lock.Open, location, location)
      val runner = JSpritRunner(getVRP, defaultSchema, Array(uav), waypoints)

      val solution = runner.solve

      val cost = JSpritRunner.getObjectiveFunction(0).getCosts(solution)
      assert((cost * 1000).toInt == 10000)

      val map = runner.getUAV2TraceMap

      val first = map.head
      val trace = List(Waypoint(first._1.currentLocation)) ++
        first._2.flatMap(_.children)

      val cost2 = spooky.getConf[UAVConf].costEstimator.estimate(trace, defaultSchema)
      assert(cost == cost2)
    }

    it("can evaluate 3 route") {
      val location = UAVConf.DEFAULT_HOME_LOCATION
      val uavs = Array("A", "B", "C").map {
        v =>
          LinkStatus(UAV(Seq(s"$v@localhost")), Lock.Open, location, location)
      }

      val runner = JSpritRunner(getVRP, defaultSchema, uavs, waypoints)

      val solution = runner.solve

      val cost = JSpritRunner.getObjectiveFunction(0).getCosts(solution)
      assert((cost * 1000).toInt == 5000)

      val map = runner.getUAV2TraceMap

      val traces = map.toSeq.map {
        v =>
          List(Waypoint(v._1.currentLocation)) ++
            v._2.flatMap(_.children)
      }

      val costs2 = traces.map {
        trace =>
          spooky.getConf[UAVConf].costEstimator.estimate(trace, defaultSchema)
      }
      val cost2 = costs2.max
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

    val tuple = JSpritRunner.solveVRP(vrp, getVRP)

    System.out.println("cost: " + tuple._2)
    assert(tuple._2 <= 1011.777)
  }
}
