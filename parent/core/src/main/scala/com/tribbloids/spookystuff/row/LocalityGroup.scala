package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import ai.acyclic.prover.commons.same.EqualBy
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.actions.Trace.Rollout

import scala.language.implicitConversions

object LocalityGroup {

  lazy val NoOp: LocalityGroup = {
    val result = LocalityGroup(Trace.NoOp)()
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
  * @param keyByOvrd
  *   custom function that affects sameness
  */
case class LocalityGroup(
    trace: Trace,
    keyByOvrd: Option[Any] = None // used by custom keyBy arg in fetch and explore.
)(
    val rollout: Rollout = Rollout(trace)
) extends EqualBy
    with SpookyContext.CanRunWith {
  // TODO: should the name be "SIMDGroup/SPMDGroup"

  @transient lazy val samenessKey: Any = keyByOvrd.getOrElse(trace)

//  def cache(vs: Seq[Observation]): this.type = {
//    rollout.cache(vs)
//    this
//  }
//
//  def uncache: this.type = {
//    rollout.uncache
//    this
//  }

  def sameBy[T](fn: Trace => T): LocalityGroup =
    this.copy(keyByOvrd = Option(fn(this.trace)))(this.rollout)

  type _WithCtx = AgentState
  val _WithCtx: SpookyContext => AgentState = { (ctx: SpookyContext) =>
    AgentState.Real(this, ctx)
  }

  def mkAgent: :=>[SpookyContext, AgentState] = withCtx
}
