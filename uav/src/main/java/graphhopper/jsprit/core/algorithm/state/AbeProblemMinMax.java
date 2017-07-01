package graphhopper.jsprit.core.algorithm.state;

import com.graphhopper.jsprit.analysis.toolbox.AlgorithmSearchProgressChartListener;
import com.graphhopper.jsprit.analysis.toolbox.Plotter;
import com.graphhopper.jsprit.core.algorithm.VehicleRoutingAlgorithm;
import com.graphhopper.jsprit.core.algorithm.box.Jsprit;
import com.graphhopper.jsprit.core.algorithm.recreate.VariableTransportCostCalculator;
import com.graphhopper.jsprit.core.algorithm.state.StateId;
import com.graphhopper.jsprit.core.algorithm.state.StateManager;
import com.graphhopper.jsprit.core.algorithm.state.StateUpdater;
import com.graphhopper.jsprit.core.algorithm.termination.VariationCoefficientTermination;
import com.graphhopper.jsprit.core.problem.VehicleRoutingProblem;
import com.graphhopper.jsprit.core.problem.constraint.ConstraintManager;
import com.graphhopper.jsprit.core.problem.constraint.SoftActivityConstraint;
import com.graphhopper.jsprit.core.problem.cost.VehicleRoutingTransportCosts;
import com.graphhopper.jsprit.io.problem.VrpXMLReader;
import com.graphhopper.jsprit.core.problem.misc.JobInsertionContext;
import com.graphhopper.jsprit.core.problem.solution.SolutionCostCalculator;
import com.graphhopper.jsprit.core.problem.solution.VehicleRoutingProblemSolution;
import com.graphhopper.jsprit.core.problem.solution.route.VehicleRoute;
import com.graphhopper.jsprit.core.problem.solution.route.activity.ActivityVisitor;
import com.graphhopper.jsprit.core.problem.solution.route.activity.End;
import com.graphhopper.jsprit.core.problem.solution.route.activity.TourActivity;
import com.graphhopper.jsprit.core.reporting.SolutionPrinter;
import com.graphhopper.jsprit.core.reporting.SolutionPrinter.Print;
import com.graphhopper.jsprit.core.util.ActivityTimeTracker;
import com.graphhopper.jsprit.core.util.CalculationUtils;
import com.graphhopper.jsprit.core.util.Solutions;
import com.graphhopper.jsprit.core.util.VehicleRoutingTransportCostsMatrix;

import java.io.IOException;
import java.util.Collection;

/*
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
class MinimaxUpdater implements ActivityVisitor, StateUpdater {

	private final StateManager stateManager;
	private final ActivityTimeTracker timeTracker;
	private final StateId stateID;

	public MinimaxUpdater(
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

class MinimiaxCost implements SolutionCostCalculator {

	private final double scalingParameter = 0.2;

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
		return maxTransportTime + scalingParameter*sumTransportTimes;
	}
}

class MinimaxConstraint implements SoftActivityConstraint {

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
		double maxTime = stateManager.getProblemState(stateId, Double.class);
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

public class AbeProblemMinMax {

	public static void main(String[] args) throws IOException {

		VehicleRoutingProblem.Builder vrpBuilder = VehicleRoutingProblem.Builder.newInstance();
		new VrpXMLReader(vrpBuilder).read("input/abe/abrahamProblem.xml");
		VehicleRoutingTransportCostsMatrix.Builder matrixBuilder = VehicleRoutingTransportCostsMatrix.Builder.newInstance(true);
		final MatrixReader matrixReader = new MatrixReader(matrixBuilder);
		matrixReader.read("input/abe/Matrix.txt");
		VehicleRoutingTransportCostsMatrix matrix = matrixBuilder.build();
		vrpBuilder.setRoutingCost(matrix);

		final VehicleRoutingProblem vrp = vrpBuilder.build();

		/*
		 * Your custom objective function that min max transport times. Additionally you can try to consider overall transport times
		 * in your objective as well. Thus you minimize max transport times first, and second, you minimize overall transport time.
		 *
		 * If you choose to consider overall transport times, makes sure you scale it appropriately.
		 */
		SolutionCostCalculator objectiveFunction = new MinimiaxCost();

		final StateManager stateManager = new StateManager(vrp);

		StateId stateId = stateManager.createStateId("max-transport-time");
		//introduce a new state called "max-transport-time"
		//add a default-state for "max-transport-time"
		stateManager.putProblemState(stateId, Double.class, 0.);
		//
		stateManager.addStateUpdater(new MinimaxUpdater(stateManager, vrp, stateId));
		
		/*
		 * The insertion heuristics is controlled with your constraints
		 */
		ConstraintManager constraintManager = new ConstraintManager(vrp, stateManager);
		// soft constraint that calculates additional transport costs when inserting a job(activity) at specified position
		constraintManager.addConstraint(new VariableTransportCostCalculator(
			vrp.getTransportCosts(),
			vrp.getActivityCosts()
		));
		/*
		 *  soft constraint that penalyzes a shift of max-route transport time, i.e. once the insertion heuristic
		 *  tries to insert a jobActivity at position which results in a shift of max-transport-time, it is penalyzed with 
		 *  penaltyForEachTimeUnitAboveCurrentMaxTime
		 *  
		 */
		SoftActivityConstraint constraint = new MinimaxConstraint(vrp, stateManager, stateId);
		constraintManager.addConstraint(constraint);

		Jsprit.Builder algorithmBuilder =  Jsprit.Builder.newInstance(vrp);
//		algorithmBuilder
		algorithmBuilder.setObjectiveFunction(objectiveFunction);

		algorithmBuilder.setStateAndConstraintManager(stateManager, constraintManager);
		algorithmBuilder.addCoreStateAndConstraintStuff(true);

		VehicleRoutingAlgorithm vra = algorithmBuilder.buildAlgorithm();

		vra.addListener(new AlgorithmSearchProgressChartListener("output/abe/progress.png"));
		VariationCoefficientTermination prematureAlgorithmTermination = new VariationCoefficientTermination(150, 0.001);
		vra.addListener(prematureAlgorithmTermination);
		vra.setPrematureAlgorithmTermination(prematureAlgorithmTermination);

		Collection<VehicleRoutingProblemSolution> solutions = vra.searchSolutions();

		Plotter plotter2 = new Plotter(vrp,Solutions.bestOf(solutions));
//		plotter2.setShowFirstActivity(true);
		plotter2.plot("output/abe/abeProblemWithSolution.png", "abe");

		SolutionPrinter.print(vrp, Solutions.bestOf(solutions), Print.VERBOSE);

		System.out.println("total-time: " + getTotalTime(vrp, Solutions.bestOf(solutions)));
		System.out.println("total-distance: " + getTotalDistance(matrixReader, Solutions.bestOf(solutions)));
	}

	private static double getTotalDistance(MatrixReader matrix,VehicleRoutingProblemSolution bestOf) {
		double dist = 0.0;
		for(VehicleRoute r : bestOf.getRoutes()){
			TourActivity last = r.getStart();
			for(TourActivity act : r.getActivities()){
				dist += matrix.getDistance(last.getLocation().getId(), act.getLocation().getId());
				last=act;
			}
			dist+=matrix.getDistance(last.getLocation().getId(), r.getEnd().getLocation().getId());
		}
		return dist;
	}

	private static double getTotalTime(VehicleRoutingProblem problem,VehicleRoutingProblemSolution bestOf) {
		double time = 0.0;
		for(VehicleRoute r : bestOf.getRoutes()) time+=r.getEnd().getArrTime();
		return time;
	}
}
