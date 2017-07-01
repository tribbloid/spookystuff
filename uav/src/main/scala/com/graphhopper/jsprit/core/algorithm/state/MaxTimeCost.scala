package com.graphhopper.jsprit.core.algorithm.state

import com.graphhopper.jsprit.core.problem.solution.route.state.RouteAndActivityStateGetter
import com.graphhopper.jsprit.core.problem.solution.{SolutionCostCalculator, VehicleRoutingProblemSolution}

/**
  * Created by peng on 6/26/17.
  */
case class MaxTimeCost(
                        stateManager: RouteAndActivityStateGetter
                      ) extends SolutionCostCalculator {

  //  override def getCosts(solution: VehicleRoutingProblemSolution): Double = {
  //    import scala.collection.JavaConverters._
  //
  //    val costs = solution.getRoutes.asScala.map {
  //      route =>
  //        stateManager.getRouteState(route, InternalStates.COSTS, classOf[Double])+
  //          getFixedCosts(route.getVehicle)
  //    }
  //
  //    var c = costs.max
  //    c += solution.getUnassignedJobs.size * c * .1
  //    c
  //  }

//  private def getFixedCosts(vehicle: Vehicle): Double = {
//    if (vehicle == null) return 0.0
//    if (vehicle.getType == null) return 0.0
//    vehicle.getType.getVehicleCostParams.fix
//  }

  private val scalingParameter = 0.2

  override def getCosts(solution: VehicleRoutingProblemSolution): Double = {
    var maxTransportTime = 0.0
    var sumTransportTimes = 0.0
    import scala.collection.JavaConversions._
    for (route <- solution.getRoutes) {
      val tpTime = route.getEnd.getArrTime - route.getStart.getEndTime
      sumTransportTimes += tpTime
      if (tpTime > maxTransportTime) maxTransportTime = tpTime
    }
    maxTransportTime + scalingParameter * sumTransportTimes
  }
}
