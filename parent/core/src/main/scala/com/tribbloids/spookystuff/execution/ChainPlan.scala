package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.row._

object ChainPlan extends CatalystTypeOps.ImplicitMixin {

  type Out[O] = Seq[Data.WithScope[O]]
  type Fn[I, O] = FetchedRow[I] => Out[O]

  type Select[I, O] = FetchedRow[I] => Seq[O]

}

case class ChainPlan[I, O]( // narrow means narrow transformation in Apache Spark
    override val child: ExecutionPlan[I],
    fn: ChainPlan.Fn[I, O]
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
