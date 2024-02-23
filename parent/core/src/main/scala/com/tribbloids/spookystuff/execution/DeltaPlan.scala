package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.execution.Delta.ToDelta
import com.tribbloids.spookystuff.row._

case class DeltaPlan(
    override val child: ExecutionPlan,
    toDelta: ToDelta
) extends UnaryPlan(child) {

  @transient lazy val delta: Delta = {
    toDelta.apply(child.outputSchema)
  }

  override def computeSchema: SpookySchema = delta.outputSchema

  final override def execute: SquashedRDD = {

    val rdd = child.squashedRDD
    val result = rdd.map(v => delta.fn(v))
    result
  }
}

object DeltaPlan extends CatalystTypeOps.ImplicitMixin {

  def optimised(
      child: ExecutionPlan,
      toDelta: ToDelta
  ): UnaryPlan = {

    child match {
      case plan: ExplorePlan if !plan.isCached =>
        plan.copy(toDeltas = plan.toDeltas :+ toDelta)
      case _ =>
        DeltaPlan(child, toDelta)
    }
  }
}
