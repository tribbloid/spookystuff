package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.multiverse.{CanEqual, Projection}
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{NoOp, Trace}
import com.tribbloids.spookystuff.actions.Trace.Rollout
import com.tribbloids.spookystuff.execution.ExecutionContext

import scala.language.implicitConversions

object LocalityGroup {

  lazy val noOp: LocalityGroup = {
    val result = LocalityGroup(NoOp)()
    result.rollout.cache(Nil)
    result
  }

  implicit def unbox(v: LocalityGroup): Trace = v.trace
}

/**
  * locality group, serve as an evidence for optimised shuffling/exploration
  *
  * deltas that share the same LocalityEv will be shuffled to be executed together more efficiently
  *
  * as a result, each SquashedRow (the smallest RDD unit) will only have 1 [[LocalityGroup]]
  *
  * @param trace
  *   static shared trace
  * @param groupKeyOvrd
  *   custom function that affects sameness
  */
case class LocalityGroup(
    trace: Trace,
    groupKeyOvrd: Option[Any] = None // used by custom keyBy arg in fetch and explore.
)(
    val rollout: Rollout = Rollout(trace)
) extends Projection.Equals
    with SpookyContext.Contextual {
  // TODO: should the name be "SIMDGroup/SPMDGroup"

  {
    canEqualProjections += CanEqual.Native.on(groupKeyOvrd.getOrElse(trace))
  }

  def sameBy[T](fn: Trace => T): LocalityGroup =
    this.copy(groupKeyOvrd = Option(fn(this.trace)))(this.rollout)

  type _WithCtx = AgentState

  override def withCtx(v: SpookyContext): AgentState = {

    AgentState.Impl(this, ExecutionContext(v))
  }
}
