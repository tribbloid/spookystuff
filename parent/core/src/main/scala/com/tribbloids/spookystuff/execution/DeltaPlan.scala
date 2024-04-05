package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.row._

object DeltaPlan extends CatalystTypeOps.ImplicitMixin {

  type Out[O] = Seq[Data.WithScope[O]]
  type Fn[I, O] = FetchedRow[I] => Out[O]

  def optimised[I, O](
      child: ExecutionPlan[I],
      fn: Fn[I, O]
  ): UnaryPlan[I, O] = {

    //    child match {
    //      case plan: ExplorePlan[I, O] if !plan.isCached =>
    //        plan.copy(deltas = plan.deltas :+ toDelta)
    //      case _ =>
    //        DeltaPlan(child, toDelta)
    //    }
    // TODO: enable optimisation later
    DeltaPlan(child, fn)
  }
}

case class DeltaPlan[I, O](
    override val child: ExecutionPlan[I],
    fn: DeltaPlan.Fn[I, O]
) extends UnaryPlan[I, O](child) {

  final override def execute: SquashedRDD[O] = {

    val rdd = child.squashedRDD
    val result = rdd.map { squashed =>
      val rows: Seq[FetchedRow[I]] = squashed.withSchema(child.outputSchema).unSquash

      val results = rows.flatMap { row =>
        fn(row)
      }

      squashed.copy(
        dataSeq = results
      )

    }
    result
  }
}
