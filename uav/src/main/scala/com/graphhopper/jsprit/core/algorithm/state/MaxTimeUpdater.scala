package com.graphhopper.jsprit.core.algorithm.state

import com.graphhopper.jsprit.core.problem.VehicleRoutingProblem
import com.graphhopper.jsprit.core.problem.cost.{ForwardTransportTime, WaitingTimeCosts}
import com.graphhopper.jsprit.core.problem.solution.route.VehicleRoute
import com.graphhopper.jsprit.core.problem.solution.route.activity.{ActivityVisitor, TourActivity}
import com.graphhopper.jsprit.core.util.ActivityTimeTracker

/**
  * Created by peng on 6/27/17.
  */
class MaxTimeUpdater(
                     stateManager: StateManager,
                     timeTracker: ActivityTimeTracker
                   ) extends ActivityVisitor with StateUpdater {

  def this(
            stateManager: StateManager,
            transportTime: ForwardTransportTime
          ) = this(
    stateManager,
    new ActivityTimeTracker(transportTime, new WaitingTimeCosts())
  )

  override def begin(route: VehicleRoute): Unit = {
    timeTracker.begin(route)
  }

  override def visit(activity: TourActivity): Unit = {
    timeTracker.visit(activity)
  }

  override def finish(): Unit = {
    timeTracker.finish()
    val newRouteEndTime = timeTracker.getActArrTime
    val currentMaxTransportTime = stateManager.getProblemState(StateFactory.createId("max-transport-time"), classOf[Double])
    if (newRouteEndTime > currentMaxTransportTime) stateManager.putProblemState(
      StateFactory.createId("max-transport-time"),
      classOf[Double],
      newRouteEndTime
    )
  }
}

object MaxTimeUpdater {

  def getStateManager(vrp: VehicleRoutingProblem): StateManager = {
    val stateManager = new StateManager(vrp)
    //introduce a new state called "max-transport-time"
    val max_transport_time_state = StateFactory.createId("max-transport-time")
    //add a default-state for "max-transport-time"
    stateManager.putProblemState(max_transport_time_state, classOf[Double], 0.0)
    //
    stateManager.addStateUpdater(new MaxTimeUpdater(stateManager, vrp.getTransportCosts))

    stateManager
  }
}
