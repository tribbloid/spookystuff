package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.Trace

object AgentState {

  def ofTrace(trace: Trace): AgentState = AgentState(LocalityGroup(trace)())
}

/**
  * [[com.tribbloids.spookystuff.session.Agent]]'s interactive API, one of the two APIs [[Delta]] can interact with
  *
  * always contains a [[LocalityGroup]] for efficient [[Delta]] execution
  *
  * always bind to exactly one [[com.tribbloids.spookystuff.session.Agent]]
  *
  * @param group
  *   shared agent interaction that improves efficiency
  */
case class AgentState(
    group: LocalityGroup
) {

  // TODO: This class is minimal before imperative/define-by-run Agent API:
  //  Delta can only interact with DataRow, interacting with Agent is not possible
  //  as a result, there is no point of multiple agents using 1 LocalityGroup, nothing needs to be shared

  def rollout: Trace.Rollout = group.rollout
}
