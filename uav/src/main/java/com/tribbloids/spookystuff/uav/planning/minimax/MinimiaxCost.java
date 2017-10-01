package com.tribbloids.spookystuff.uav.planning.minimax;

import com.graphhopper.jsprit.core.problem.solution.SolutionCostCalculator;
import com.graphhopper.jsprit.core.problem.solution.VehicleRoutingProblemSolution;
import com.graphhopper.jsprit.core.problem.solution.route.VehicleRoute;

/**
 * Created by peng on 7/2/17.
 */
public class MinimiaxCost implements SolutionCostCalculator {

	private double cohesiveness = 0.05;

//	public MinimiaxCost() {
//	}

	public MinimiaxCost(double cohesiveness) {
		this.cohesiveness = cohesiveness;
	}

	@Override
	public double getCosts(VehicleRoutingProblemSolution solution) {
		double maxTransportTime = 0.;
		double sumTransportTimes = 0.;
		for(VehicleRoute route : solution.getRoutes()){
			double tpTime = route.getEnd().getArrTime() - route.getStart().getEndTime();
			sumTransportTimes+=tpTime;
			if(tpTime > maxTransportTime){
				maxTransportTime = tpTime;
			}
		}
		return maxTransportTime + cohesiveness *sumTransportTimes;
	}
}
