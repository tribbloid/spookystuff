package com.tribbloids.spookystuff.uav.planning;

import com.graphhopper.jsprit.core.algorithm.state.StateId;
import com.graphhopper.jsprit.core.algorithm.state.StateManager;
import com.graphhopper.jsprit.core.problem.VehicleRoutingProblem;
import com.graphhopper.jsprit.core.problem.constraint.SoftActivityConstraint;
import com.graphhopper.jsprit.core.problem.cost.VehicleRoutingTransportCosts;
import com.graphhopper.jsprit.core.problem.misc.JobInsertionContext;
import com.graphhopper.jsprit.core.problem.solution.route.activity.End;
import com.graphhopper.jsprit.core.problem.solution.route.activity.TourActivity;
import com.graphhopper.jsprit.core.util.CalculationUtils;

/**
 * Created by peng on 7/2/17.
 */
public class MinimaxConstraint implements SoftActivityConstraint {

  private final VehicleRoutingProblem problem;
  private final StateManager stateManager;
  private final VehicleRoutingTransportCosts routingCosts;
  private final StateId stateId;

  MinimaxConstraint(
          VehicleRoutingProblem problem,
          StateManager stateManager,
          StateId stateId
  ) {
    super();
    this.problem = problem;
    this.stateManager = stateManager;
    this.routingCosts = problem.getTransportCosts();
    this.stateId = stateId;
  }

  static final double penaltyForEachTimeUnitAboveCurrentMaxTime = 3.;

  @Override
  public double getCosts(JobInsertionContext iFacts, TourActivity prevAct, TourActivity newAct, TourActivity nextAct, double depTimeAtPrevAct) {
				/*
				 * determines maximum of all routes' transport times, which is here basically a state that can be fetched via the stateManager
				 */
    double maxTime = 0.0;
    try {
      stateManager.getProblemState(stateId, Double.class);
    }
    catch(NullPointerException ignored) {
    }
				/*
				 * determines additional time of route when inserting newAct between prevAct and nextAct
				 *
				 */
    double tp_time_prevAct_newAct = routingCosts.getTransportTime(prevAct.getLocation(), newAct.getLocation(), depTimeAtPrevAct, iFacts.getNewDriver(), iFacts.getNewVehicle());
    double newAct_arrTime = depTimeAtPrevAct + tp_time_prevAct_newAct;
    double newAct_endTime = CalculationUtils.getActivityEndTime(newAct_arrTime, newAct);
				/*
				 * open routes - if route is set to be open, i.e. end is endogeneously determined by the algorithm, then inserting of newAct between prevAct
				 * and end just shifts the route's end time to by tp_time_prevAct_newAct
				 *
				 */
    if(nextAct instanceof End){
      if(!iFacts.getNewVehicle().isReturnToDepot()){
        double additionalTime = tp_time_prevAct_newAct;
        double new_routes_transport_time = iFacts.getRoute().getEnd().getArrTime() - iFacts.getRoute().getStart().getEndTime() + additionalTime;
        return penaltyForEachTimeUnitAboveCurrentMaxTime*Math.max(0,new_routes_transport_time-maxTime);
      }
    }
    double tp_time_newAct_nextAct = routingCosts.getTransportTime(newAct.getLocation(), nextAct.getLocation(), newAct_endTime, iFacts.getNewDriver(), iFacts.getNewVehicle());
    double nextAct_arrTime = newAct_endTime + tp_time_newAct_nextAct;
    double oldTime;
    if(iFacts.getRoute().isEmpty()){
      oldTime = (nextAct.getArrTime() - depTimeAtPrevAct);
    }
    else{
      oldTime = (nextAct.getArrTime() - iFacts.getRoute().getDepartureTime());
    }
    double additionalTime = (nextAct_arrTime - iFacts.getNewDepTime()) - oldTime;
    double tpTime = iFacts.getRoute().getEnd().getArrTime() - iFacts.getRoute().getStart().getEndTime() + additionalTime;

    return penaltyForEachTimeUnitAboveCurrentMaxTime*Math.max(0,tpTime-maxTime);
  }
}
