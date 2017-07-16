package com.tribbloids.spookystuff.uav.planning;

import com.graphhopper.jsprit.analysis.toolbox.AlgorithmSearchProgressChartListener;
import com.graphhopper.jsprit.analysis.toolbox.Plotter;
import com.graphhopper.jsprit.core.algorithm.VehicleRoutingAlgorithm;
import com.graphhopper.jsprit.core.algorithm.box.Jsprit;
import com.graphhopper.jsprit.core.algorithm.recreate.VariableTransportCostCalculator;
import com.graphhopper.jsprit.core.algorithm.state.StateId;
import com.graphhopper.jsprit.core.algorithm.state.StateManager;
import com.graphhopper.jsprit.core.algorithm.termination.VariationCoefficientTermination;
import com.graphhopper.jsprit.core.problem.VehicleRoutingProblem;
import com.graphhopper.jsprit.core.problem.constraint.ConstraintManager;
import com.graphhopper.jsprit.core.problem.constraint.SoftActivityConstraint;
import com.graphhopper.jsprit.core.problem.solution.SolutionCostCalculator;
import com.graphhopper.jsprit.core.problem.solution.VehicleRoutingProblemSolution;
import com.graphhopper.jsprit.core.problem.solution.route.VehicleRoute;
import com.graphhopper.jsprit.core.problem.solution.route.activity.TourActivity;
import com.graphhopper.jsprit.core.reporting.SolutionPrinter;
import com.graphhopper.jsprit.core.reporting.SolutionPrinter.Print;
import com.graphhopper.jsprit.core.util.Solutions;
import com.graphhopper.jsprit.core.util.VehicleRoutingTransportCostsMatrix;
import com.graphhopper.jsprit.io.problem.VrpXMLReader;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

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
//		stateManager.putProblemState(stateId, Double.class, 0.);
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

        File file = new File("log/abe/");
        if (!file.exists()) file.mkdirs();

        vra.addListener(new AlgorithmSearchProgressChartListener("log/abe/progress.png"));
        VariationCoefficientTermination prematureAlgorithmTermination = new VariationCoefficientTermination(150, 0.001);
        vra.addListener(prematureAlgorithmTermination);
        vra.setPrematureAlgorithmTermination(prematureAlgorithmTermination);

        Collection<VehicleRoutingProblemSolution> solutions = vra.searchSolutions();

        VehicleRoutingProblemSolution best = Solutions.bestOf(solutions);

        Plotter plotter2 = new Plotter(vrp, best);
//		plotter2.setShowFirstActivity(true);
        plotter2.plot("log/abe/abeProblemWithSolution.png", "abe");
        SolutionPrinter.print(vrp, best, Print.VERBOSE);

        System.out.println("total-time: " + getTotalTime(vrp, best));
        System.out.println("total-distance: " + getTotalDistance(matrixReader, best));
        System.out.println("cost: " + objectiveFunction.getCosts(best));
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
