package com.graphhopper.jsprit.core.algorithm.state

import com.graphhopper.jsprit.core.problem.VehicleRoutingProblem
import com.graphhopper.jsprit.core.problem.constraint.SoftActivityConstraint
import com.graphhopper.jsprit.core.problem.misc.JobInsertionContext
import com.graphhopper.jsprit.core.problem.solution.route.activity.{End, TourActivity}
import com.graphhopper.jsprit.core.util.CalculationUtils

/**
  * Created by peng on 6/25/17.
  */
case class MaxTimeConstraint(
                              problem: VehicleRoutingProblem,
                              stateManager: StateManager
                            ) extends SoftActivityConstraint {

  final private val routingCosts = problem.getTransportCosts
  final private val penaltyForEachTimeUnitAboveCurrentMaxTime = 3.0

  override def getCosts(
                         iFacts: JobInsertionContext,
                         prevAct: TourActivity,
                         newAct: TourActivity,
                         nextAct: TourActivity,
                         depTimeAtPrevAct: Double
                       ): Double = {

    /*
     * determines maximum of all routes' transport times, which is here basically a state that can be fetched via the stateManager
     */
    val maxTime = stateManager.getProblemState(stateManager.createStateId("max-transport-time"), classOf[Double])

    /*
     * determines additional time of route when inserting newAct between prevAct and nextAct
     */
    val tp_time_prevAct_newAct = routingCosts.getTransportTime(prevAct.getLocation, newAct.getLocation, depTimeAtPrevAct, iFacts.getNewDriver, iFacts.getNewVehicle)

    val newAct_arrTime = depTimeAtPrevAct + tp_time_prevAct_newAct
    //TODO: where is the deprecation replacement ?
    val newAct_endTime = CalculationUtils.getActivityEndTime(newAct_arrTime, newAct)

    /*
     * open routes - if route is set to be open, i.e. end is endogeneously determined by the algorithm, then inserting of newAct between prevAct
     * and end just shifts the route's end time to by tp_time_prevAct_newAct
     */
    if (nextAct.isInstanceOf[End]) if (!iFacts.getNewVehicle.isReturnToDepot) {
      val additionalTime = tp_time_prevAct_newAct
      val new_routes_transport_time = iFacts.getRoute.getEnd.getArrTime - iFacts.getRoute.getStart.getEndTime + additionalTime
      return penaltyForEachTimeUnitAboveCurrentMaxTime * Math.max(0, new_routes_transport_time - maxTime)
    }
    val tp_time_newAct_nextAct = routingCosts.getTransportTime(newAct.getLocation, nextAct.getLocation, newAct_endTime, iFacts.getNewDriver, iFacts.getNewVehicle)
    val nextAct_arrTime = newAct_endTime + tp_time_newAct_nextAct
    var oldTime = .0
    if (iFacts.getRoute.isEmpty) oldTime = nextAct.getArrTime - depTimeAtPrevAct
    else oldTime = nextAct.getArrTime - iFacts.getRoute.getDepartureTime
    val additionalTime = (nextAct_arrTime - iFacts.getNewDepTime) - oldTime
    val tpTime = iFacts.getRoute.getEnd.getArrTime - iFacts.getRoute.getStart.getEndTime + additionalTime
    penaltyForEachTimeUnitAboveCurrentMaxTime * Math.max(0, tpTime - maxTime)
  }
}
