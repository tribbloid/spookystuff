package com.tribbloids.spookystuff.uav.planning.VRPOptimizers

import java.io.File

import com.graphhopper.jsprit.core.problem.{Location => JLocation}
import com.graphhopper.jsprit.analysis.toolbox.{AlgorithmSearchProgressChartListener, Plotter}
import com.graphhopper.jsprit.core.algorithm.VehicleRoutingAlgorithm
import com.graphhopper.jsprit.core.algorithm.box.Jsprit
import com.graphhopper.jsprit.core.algorithm.recreate.VariableTransportCostCalculator
import com.graphhopper.jsprit.core.algorithm.state.{StateId, StateManager}
import com.graphhopper.jsprit.core.algorithm.termination.VariationCoefficientTermination
import com.graphhopper.jsprit.core.problem.{Capacity, VehicleRoutingProblem}
import com.graphhopper.jsprit.core.problem.constraint.{ConstraintManager, SoftActivityConstraint}
import com.graphhopper.jsprit.core.problem.job.Service
import com.graphhopper.jsprit.core.problem.solution.{SolutionCostCalculator, VehicleRoutingProblemSolution}
import com.graphhopper.jsprit.core.problem.vehicle.{VehicleImpl, VehicleTypeImpl}
import com.graphhopper.jsprit.core.reporting.SolutionPrinter
import com.graphhopper.jsprit.core.reporting.SolutionPrinter.Print
import com.graphhopper.jsprit.core.util.{Coordinate, FastVehicleRoutingTransportCostsMatrix, Solutions}
import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.{UAVNavigation, Waypoint}
import com.tribbloids.spookystuff.uav.dsl.GenPartitioners
import com.tribbloids.spookystuff.uav.spatial.point.NED
import com.tribbloids.spookystuff.uav.telemetry.LinkStatus

import scala.util.Try

object JSpritRunner {

  def getObjectiveFunction(cohesiveness: Double): SolutionCostCalculator =
    new MinimiaxCost(cohesiveness)

  def getCostMatrix(
      schema: SpookySchema,
      trace_indices: Seq[(TraceView, Int)]
  ): FastVehicleRoutingTransportCostsMatrix = {

    val costEstimator = schema.ec.spooky.getConf[UAVConf].costEstimator

    val dMat =
      for (
        i <- trace_indices;
        j <- trace_indices
      ) yield {
        if (i._2 == j._2)
          (i._2, j._2, 0.0)
        else {
          val traceView: TraceView = i._1
          val trace = traceView.children
          val last = trace.collect { case v: UAVNavigation => v }.last
          val lastLocation = last.getEnd(schema)
          val realTrace = List(Waypoint(lastLocation)) ++ j._1.children
          val cost = costEstimator.estimate(realTrace, schema)
          (i._2, j._2, cost)
        }
      }

    val size = trace_indices.length
    val jRoutingCostMat: FastVehicleRoutingTransportCostsMatrix = {
      val builder = FastVehicleRoutingTransportCostsMatrix.Builder
        .newInstance(size, false)
      dMat.foreach { entry =>
        builder.addTransportTimeAndDistance(entry._1, entry._2, entry._3, entry._3)
      }
      builder.build()
    }
    jRoutingCostMat
  }

  def solveVRP(
      vrp: VehicleRoutingProblem,
      gp: GenPartitioners.VRP
  ): (VehicleRoutingProblemSolution, Double) = {

    val stateManager: StateManager = new StateManager(vrp)

    val stateId: StateId = stateManager.createStateId("max-transport-time")
    // introduce a new state called "max-transport-time"
    // add a default-state for "max-transport-time"
    //    stateManager.putProblemState(stateId, classOf[Double], 0.0)
    //
    stateManager.addStateUpdater(new MinimaxStateUpdater(stateManager, vrp, stateId))

    /*
     * The insertion heuristics is controlled with your constraints
     */
    val constraintManager: ConstraintManager = new ConstraintManager(vrp, stateManager)
    // soft constraint that calculates additional transport costs when inserting a job(activity) at specified position
    constraintManager.addConstraint(new VariableTransportCostCalculator(vrp.getTransportCosts, vrp.getActivityCosts))
    /*
     *  soft constraint that penalyzes a shift of max-route transport time, i.e. once the insertion heuristic
     *  tries to insert a jobActivity at position which results in a shift of max-transport-time, it is penalyzed with
     *  penaltyForEachTimeUnitAboveCurrentMaxTime
     *
     */
    val constraint: SoftActivityConstraint = new MinimaxConstraint(vrp, stateManager, stateId)
    constraintManager.addConstraint(constraint)

    val algorithmBuilder: Jsprit.Builder = Jsprit.Builder.newInstance(vrp)
    //		algorithmBuilder
    val objectiveFunction = JSpritRunner.getObjectiveFunction(gp.cohesiveness)
    algorithmBuilder.setObjectiveFunction(objectiveFunction)

    algorithmBuilder.setStateAndConstraintManager(stateManager, constraintManager)
    algorithmBuilder.addCoreStateAndConstraintStuff(true)

    val vra: VehicleRoutingAlgorithm = algorithmBuilder.buildAlgorithm

    gp.covergencePlotPathOpt.foreach { v =>
      val file = new File(v)
      if (!file.exists()) file.getParentFile.mkdirs()
      vra.addListener(new AlgorithmSearchProgressChartListener(v))
    }
    val prematureAlgorithmTermination: VariationCoefficientTermination = new VariationCoefficientTermination(150, 0.001)
    vra.addListener(prematureAlgorithmTermination)
    vra.setPrematureAlgorithmTermination(prematureAlgorithmTermination)

    val solutions = vra.searchSolutions

    val best = Solutions.bestOf(solutions)

    SolutionPrinter.print(vrp, best, Print.VERBOSE)
    gp.solutionPlotPathOpt.foreach { v =>
      plot(vrp, best, v)
    }

    best -> objectiveFunction.getCosts(best)
  }

  def getPlotCoord(trace: Trace, schema: SpookySchema): NED.Coordinate = {
    val navs: Seq[UAVNavigation] = trace.collect {
      case nav: UAVNavigation => nav
    }
    val home = schema.ec.spooky.getConf[UAVConf]._home
    for (nav <- navs) {
      val opt = Try(nav.getLocation(schema)).toOption.flatMap {
        _.getCoordinate(NED, home)
      }
      if (opt.nonEmpty) return opt.get
    }
    NED(navs.size, 0, 0)
  }

  def plot(
      vrp: VehicleRoutingProblem,
      solution: VehicleRoutingProblemSolution,
      path: String,
      title: String = "JSprit"
  ): Unit = {

    val file = new File(path)
    if (!file.exists()) file.getParentFile.mkdirs()

    val plotter2: Plotter = new Plotter(vrp, solution)
    //		plotter2.setShowFirstActivity(true);

    plotter2.plot(path, title)
  }
}

case class JSpritRunner(
    problem: GenPartitioners.VRP,
    schema: SpookySchema,
    uavs: Array[LinkStatus],
    traces: Array[TraceView]
) {

  val spooky = schema.ec.spooky

  val trace_uavOpt_index: Array[((TraceView, Option[LinkStatus]), Int)] = {
    val fromUAVs: Array[(TraceView, Option[LinkStatus])] =
      uavs.map { uav =>
        TraceView(List(Waypoint(uav.currentLocation))) -> Some(uav)
      }

    val fromTraces: Array[(TraceView, Option[LinkStatus])] = traces.map { trace =>
      trace -> None
    }

    (fromUAVs ++ fromTraces).zipWithIndex
  }

  val homeLocation = spooky.getConf[UAVConf]._home

  lazy val define: VehicleRoutingProblem = {

    val trace_indices: Array[(TraceView, Int)] = trace_uavOpt_index.map { triplet =>
      triplet._1._1 -> triplet._2
    }

    val jRoutingCostMat: FastVehicleRoutingTransportCostsMatrix =
      JSpritRunner.getCostMatrix(schema, trace_indices)

    val jVehicles: Array[VehicleImpl] = getJVehicles

    val jServices: Array[Service] = getJServices

    val vrp = {
      val builder = VehicleRoutingProblem.Builder
        .newInstance()
        .setRoutingCost(jRoutingCostMat)
      for (v <- jVehicles) {
        builder.addVehicle(v)
      }
      for (s <- jServices) {
        builder.addJob(s)
      }
      builder.setFleetSize(VehicleRoutingProblem.FleetSize.FINITE)
      builder.build()
    }
    vrp
  }

  def getJVehicles: Array[VehicleImpl] = {
    val cap = Capacity.Builder
      .newInstance()
      .addDimension(0, 1)
      .build()
    val jVType = VehicleTypeImpl.Builder
      .newInstance("UAV")
      .setCapacityDimensions(cap)
      .build()

    val jVehicles = trace_uavOpt_index
      .flatMap { triplet =>
        triplet._1._2.map { v =>
          v -> triplet._2
        }
      }
      .map { tuple =>
        val status = tuple._1
        val location = status.currentLocation
        val coord = location.getCoordinate(NED, homeLocation).get
        val jLocation = JLocation.Builder
          .newInstance()
          .setIndex(tuple._2)
          .setCoordinate(
            Coordinate.newInstance(
              coord.east,
              coord.north
            )
          )
          .build()
        val jVehicle = VehicleImpl.Builder
          .newInstance(status.uav.primaryURI)
          .setType(jVType)
          .setStartLocation(jLocation)
          .setReturnToDepot(false)
          .build()
        jVehicle
      }
    jVehicles
  }

  def getJServices: Array[Service] = {
    val jServices: Array[Service] = trace_uavOpt_index
      .flatMap { triplet =>
        triplet._1._2 match {
          case Some(_) =>
            None
          case None =>
            Some(triplet._1._1 -> triplet._2)
        }
      }
      .map { tuple =>
        val trace = tuple._1.children

        val plotCoord = JSpritRunner.getPlotCoord(trace, schema)
        val location = JLocation.Builder
          .newInstance()
          .setIndex(tuple._2)
          .setCoordinate(
            Coordinate.newInstance(
              plotCoord.east,
              plotCoord.north
            )
          )
          .build()

        Service.Builder
          .newInstance(tuple._1.hashCode().toString)
          .setLocation(location)
          .build()
      }
    jServices
  }

  // TODO: need independent test
  // TODO: why not use shipment? has better visualization.
  lazy val solve: VehicleRoutingProblemSolution = {

    val vrp: VehicleRoutingProblem = define

    val tuple = JSpritRunner.solveVRP(vrp, problem)
    println(s"cost = ${tuple._2}")
    tuple._1
  }

  lazy val getUAV2TraceMap: Map[LinkStatus, Seq[TraceView]] = {

    import scala.collection.JavaConverters._

    val routes = solve.getRoutes.asScala.toList
    val status_KVs: Seq[(LinkStatus, List[TraceView])] = routes.map { route =>
      val status = uavs.find(_.uav.primaryURI == route.getVehicle.getId).get
      val tours = route.getTourActivities.getActivities.asScala.toList
      val traces = for (tour <- tours) yield {
        val index = tour.getLocation.getIndex
        val trace: TraceView = trace_uavOpt_index.find(_._2 == index).get._1._1
        trace
      }
      status -> traces
    }
    val status_KVMap = Map(status_KVs: _*)
    status_KVMap
  }
}
