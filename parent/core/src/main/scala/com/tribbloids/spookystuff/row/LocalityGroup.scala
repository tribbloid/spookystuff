package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.same.EqualBy
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.actions.Trace.Rollout
import com.tribbloids.spookystuff.commons.serialization.NOTSerializable
import com.tribbloids.spookystuff.doc.Observation

import scala.collection.MapView
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

  @transient lazy val samenessKey: Any = keyByOvrd.getOrElse(trace)

  def unCache: LocalityGroup = this.copy()(rollout.unCache)

  def sameBy[T](fn: Trace => T): LocalityGroup =
    this.copy(keyByOvrd = Option(fn(this.trace)))(this.rollout)

  case class _WithCtx(spooky: SpookyContext) extends NOTSerializable {

    lazy val trajectory: Seq[Observation] = rollout.withCtx(spooky).trajectory

    lazy val lookupToN: Map[Observation.DocUID, Seq[Observation]] = {

      trajectory.groupBy { oo =>
        oo.uid
      }
    }

    lazy val lookup: MapView[Observation.DocUID, Observation] = lookupToN.view.mapValues { v =>
      require(v.size == 1, "multiple observations with identical UID")
      v.head
    }
  }

}
