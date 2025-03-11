package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.multiverse.{CanEqual, Projection}
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{NoOp, Trace}
import com.tribbloids.spookystuff.actions.Trace.Rollout
import com.tribbloids.spookystuff.execution.ExecutionContext

import scala.language.implicitConversions

object LocalityGroup {

  lazy val noOp: LocalityGroup = {
    val result = LocalityGroup(NoOp.trace)
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
  * @param keyOvrd
  *   custom function that affects sameness
  */
case class LocalityGroup(
    rollout: Rollout,
    keyOvrd: Option[Any] = None // used by custom keyBy arg in fetch and explore.

) extends Projection.Equals
    with SpookyContext.Contextual {
  // TODO: should the name be "SIMDGroup/SPMDGroup"

  {
    canEqualProjections += CanEqual.Native.on(keyOvrd.getOrElse(trace))
  }

  def trace = rollout.trace

  def sameBy[T](fn: Trace => T): LocalityGroup =
    this.copy(keyOvrd = Option(fn(this.trace)))

  type _WithCtx = AgentState

  override protected def _WithCtx(v: SpookyContext): AgentState = {

    AgentState.Impl(this, ExecutionContext(v))
  }
}
