package com.tribbloids.spookystuff.uav.planning

import com.graphhopper.jsprit.analysis.toolbox.{AlgorithmSearchProgressChartListener, Plotter}
import com.graphhopper.jsprit.core.algorithm.VehicleRoutingAlgorithm
import com.graphhopper.jsprit.core.algorithm.box.Jsprit
import com.graphhopper.jsprit.core.algorithm.recreate.VariableTransportCostCalculator
import com.graphhopper.jsprit.core.algorithm.state.{StateId, StateManager}
import com.graphhopper.jsprit.core.algorithm.termination.VariationCoefficientTermination
import com.graphhopper.jsprit.core.problem.constraint.{ConstraintManager, SoftActivityConstraint}
import com.graphhopper.jsprit.core.problem.job.Service
import com.graphhopper.jsprit.core.problem.solution.{SolutionCostCalculator, VehicleRoutingProblemSolution}
import com.graphhopper.jsprit.core.problem.vehicle.{VehicleImpl, VehicleTypeImpl}
import com.graphhopper.jsprit.core.problem.{Capacity, VehicleRoutingProblem, Location => JLocation}
import com.graphhopper.jsprit.core.reporting.SolutionPrinter
import com.graphhopper.jsprit.core.reporting.SolutionPrinter.Print
import com.graphhopper.jsprit.core.util.{Coordinate, FastVehicleRoutingTransportCostsMatrix, Solutions}
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.spatial.NED
import com.tribbloids.spookystuff.uav.telemetry.UAVStatus

/**
  * Created by peng on 7/2/17.
  */
object JSpritSolver {

  // TODO: need independent test
  def vrpSolution(
                   spooky: SpookyContext,
                   trace_uavOpt_index: Array[((TraceView, Option[UAVStatus]), Int)]
                 ): VehicleRoutingProblemSolution = {

    val homeLocation = spooky.getConf[UAVConf].homeLocation

    val trace_indices: Array[(TraceView, Int)] = trace_uavOpt_index.map {
      triplet =>
        triplet._1._1 -> triplet._2
    }

    val jRoutingCostMat: FastVehicleRoutingTransportCostsMatrix = getCostMatrix(spooky, trace_indices)

    val cap = Capacity.Builder.newInstance()
      .addDimension(0, 1)
      .build()
    val jVType = VehicleTypeImpl.Builder.newInstance("UAV")
      .setCapacityDimensions(cap)
      .build()

    val jVehicles = trace_uavOpt_index
      .flatMap {
        triplet =>
          triplet._1._2.map { v => v -> triplet._2 }
      }
      .map {
        tuple =>
          val status = tuple._1
          val location = status.currentLocation
          val coord = location.getCoordinate(NED, homeLocation).get
          val jLocation = JLocation.Builder.newInstance()
            .setIndex(tuple._2)
            .setCoordinate(
              Coordinate.newInstance(
                coord.east,
                coord.north
              )
            )
            .build()
          val jVehicle = VehicleImpl.Builder
            .newInstance(status.uav.fullID)
            .setType(jVType)
            .setStartLocation(jLocation)
            .build()
          jVehicle
      }

    //TODO: why not use shipment? has better visualization.
    val jobs: Array[Service] = trace_uavOpt_index
      .flatMap {
        triplet =>
          triplet._1._2 match {
            case Some(_) =>
              None
            case None =>
              Some(triplet._1._1 -> triplet._2)
          }
      }
      .map {
        tuple =>
          val navs: Seq[UAVNavigation] = tuple._1.children.collect {
            case nav: UAVNavigation => nav
          }

          val coord = navs.head._from.getCoordinate(NED, homeLocation).get
          val location = JLocation.Builder
            .newInstance()
            .setIndex(tuple._2)
            .setCoordinate(
              Coordinate.newInstance(
                coord.east,
                coord.north
              )
            )
            .build()

          Service.Builder.newInstance(tuple._1.TreeNode.treeString)
            .setLocation(location)
            .build()
      }
    val vrp = {
      val builder = VehicleRoutingProblem.Builder.newInstance()
        .setRoutingCost(jRoutingCostMat)
      for (v <- jVehicles) {
        builder.addVehicle(v)
      }
      for (s <- jobs) {
        builder.addJob(s)
      }
      builder.build()
    }

    /*
         * Your custom objective function that min max transport times. Additionally you can try to consider overall transport times
         * in your objective as well. Thus you minimize max transport times first, and second, you minimize overall transport time.
         *
         * If you choose to consider overall transport times, makes sure you scale it appropriately.
         */
    val objectiveFunction: SolutionCostCalculator = new MinimiaxCost

    val stateManager: StateManager = new StateManager(vrp)

    val stateId: StateId = stateManager.createStateId("max-transport-time")
    //introduce a new state called "max-transport-time"
    //add a default-state for "max-transport-time"
    //    stateManager.putProblemState(stateId, classOf[Double], 0.0)
    //
    stateManager.addStateUpdater(new MinimaxUpdater(stateManager, vrp, stateId))

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
    algorithmBuilder.setObjectiveFunction(objectiveFunction)

    algorithmBuilder.setStateAndConstraintManager(stateManager, constraintManager)
    algorithmBuilder.addCoreStateAndConstraintStuff(true)

    val vra: VehicleRoutingAlgorithm = algorithmBuilder.buildAlgorithm

    vra.addListener(new AlgorithmSearchProgressChartListener("output/abe/progress.png"))
    val prematureAlgorithmTermination: VariationCoefficientTermination = new VariationCoefficientTermination(150, 0.001)
    vra.addListener(prematureAlgorithmTermination)
    vra.setPrematureAlgorithmTermination(prematureAlgorithmTermination)

    val solutions = vra.searchSolutions

    val best = Solutions.bestOf(solutions)
    val plotter2: Plotter = new Plotter(vrp, best)
    //		plotter2.setShowFirstActivity(true);
    plotter2.plot("output/abe/abeProblemWithSolution.png", "abe")

    SolutionPrinter.print(vrp, best, Print.VERBOSE)

    //    System.out.println("total-time: " + getTotalTime(vrp, Solutions.bestOf(solutions)))
    //    System.out.println("total-distance: " + getTotalDistance(matrixReader, Solutions.bestOf(solutions)))



    //    val stateManager = MaxTimeUpdater.getStateManager(vrp)
    //
    //    val constraintManager = new ConstraintManager(vrp, stateManager)
    //    // soft constraint that calculates additional transport costs when inserting a job(activity) at specified position
    //    constraintManager.addConstraint(
    //      new VariableTransportCostCalculator(vrp.getTransportCosts, vrp.getActivityCosts)
    //    )
    //
    //    val objectiveFn = MaxTimeCost(stateManager)
    //
    //    val vra = Jsprit.Builder.newInstance(vrp)
    //      .setObjectiveFunction(objectiveFn)
    //      .setStateAndConstraintManager(stateManager, constraintManager)
    //      .addCoreStateAndConstraintStuff(true)
    //      .buildAlgorithm()
    //
    //    val solutions = vra.searchSolutions
    //    val best = Solutions.bestOf(solutions)
    best
  }

  def getCostMatrix(spooky: SpookyContext, trace_indices: Array[(TraceView, Int)]) = {
    val costEstimator = spooky.getConf[UAVConf].costEstimator

    val dMat = for (
      i <- trace_indices;
      j <- trace_indices
    ) yield {
      val traceView: TraceView = i._1
      val last = traceView.children.collect { case v: UAVNavigation => v }.last
      val lastLocation = last._to
      val cost = costEstimator.estimate(
        List(WrapLocation(lastLocation)) ++ j._1.children,
        spooky
      )
      (i._2, j._2, cost)
    }

    val size = trace_indices.length
    val jRoutingCostMat: FastVehicleRoutingTransportCostsMatrix = {
      val builder = FastVehicleRoutingTransportCostsMatrix.Builder
        .newInstance(size, false)
      dMat.foreach {
        entry =>
          builder.addTransportDistance(entry._1, entry._2, entry._3)
      }
      builder.build()
    }
    jRoutingCostMat
  }
}
