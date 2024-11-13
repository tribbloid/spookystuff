package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.commons.serialization.NOTSerializable
import com.tribbloids.spookystuff.doc.Observation

import scala.collection.MapView

object AgentState {

  case class Real( // TODO: merge into LocalityGroup.WithCtx
      group: LocalityGroup,
      ctx: SpookyContext
  ) extends AgentState {

    // TODO: This class is minimal before imperative/define-by-run Agent API:
    //  Delta can only interact with DataRow, interacting with Agent is not possible
    //  as a result, there is no point of multiple agents using 1 LocalityGroup, nothing needs to be shared

    def rollout: Trace.Rollout = group.rollout

    lazy val trajectory: Seq[Observation] = rollout.withCtx(ctx).trajectory
  }

//  case class Mock(
//      trajectory: Seq[Observation],
//      ctx: SpookyContext
//  ) extends AgentState {}
}

/**
  * [[com.tribbloids.spookystuff.agent.Agent]]'s interactive API, one of the two APIs [[Delta]] can interact with
  *
  * always contains a [[LocalityGroup]] for efficient [[Delta]] execution
  *
  * always bind to exactly one [[com.tribbloids.spookystuff.agent.Agent]]
  *
  * @param group
  *   shared agent interaction that improves efficiency
  */
trait AgentState extends NOTSerializable {

  // TODO: This class is minimal before imperative/define-by-run Agent API:
  //  Delta can only interact with DataRow, interacting with Agent is not possible
  //  as a result, there is no point of multiple agents using 1 LocalityGroup, nothing needs to be shared

  def ctx: SpookyContext
  def trajectory: Seq[Observation]

  protected lazy val lookup_multi: Map[Observation.DocUID, Seq[Observation]] = {

    trajectory.groupBy { oo =>
      oo.uid
    }
  }

  lazy val lookup: MapView[Observation.DocUID, Observation] = lookup_multi.view.mapValues { v =>
    require(v.size == 1, "multiple observations with identical UID")
    v.head
  }
}
