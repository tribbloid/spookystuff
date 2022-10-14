package com.tribbloids.spookystuff.uav.planning.VRPOptimizers;

import com.graphhopper.jsprit.core.algorithm.state.StateId;
import com.graphhopper.jsprit.core.algorithm.state.StateManager;
import com.graphhopper.jsprit.core.algorithm.state.StateUpdater;
import com.graphhopper.jsprit.core.problem.VehicleRoutingProblem;
import com.graphhopper.jsprit.core.problem.solution.route.VehicleRoute;
import com.graphhopper.jsprit.core.problem.solution.route.activity.ActivityVisitor;
import com.graphhopper.jsprit.core.problem.solution.route.activity.TourActivity;
import com.graphhopper.jsprit.core.util.ActivityTimeTracker;

/**
 * This updates the state "max-transport-time" which is introduced below. Once either the insertion procedure starts or a job has
 * been inserted, UpdateMaxTransportTime is called for the route that has been changed.
 *
 * It must not only be an ActivityVisitor which indicates that the update procedure starts at the beginning of route all the way to end
 * (in contrary to the ReverseActivityVisitor) but also be a StateUpdater which is just a marker to register it in the StateManager.
 *
 * You do not need to declare this as static inner class. You can just choose your preferred approach. However, be aware
 * that this requires the stateName "max-transport-time" you define below. If you choose to define this as class in a new file,
 * you might define "max-transport-time" as static id in another file, to make sure you do not have type errors etc..
 */
public class MinimaxStateUpdater implements ActivityVisitor, StateUpdater {

	private final StateManager stateManager;
	private final ActivityTimeTracker timeTracker;
	private final StateId stateID;

	public MinimaxStateUpdater(
		StateManager stateManager,
		VehicleRoutingProblem vrp,
		StateId stateID
	) {
		super();
		this.stateManager = stateManager;
		this.timeTracker = new ActivityTimeTracker(vrp.getTransportCosts(), vrp.getActivityCosts());
		this.stateID = stateID;
	}

	@Override
	public void begin(VehicleRoute route) {
		timeTracker.begin(route);
	}

	@Override
	public void visit(TourActivity activity) {
		timeTracker.visit(activity);
	}

	@Override
	public void finish() {
		timeTracker.finish();
		double newRouteEndTime = timeTracker.getActArrTime();
		double currentMaxTransportTime = 0.0;
		try {
			currentMaxTransportTime = stateManager.getProblemState(stateID, Double.class);
		}
		catch (NullPointerException ignored) {
		}
		if(newRouteEndTime > currentMaxTransportTime){
			stateManager.putProblemState(stateID, Double.class, newRouteEndTime);
		}
	}
}
