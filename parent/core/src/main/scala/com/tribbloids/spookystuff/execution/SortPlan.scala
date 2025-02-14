package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.util.Magnet.OptionMagnet
import com.tribbloids.spookystuff.row.{FetchedRow, SquashedRDD, SquashedRow}

import scala.reflect.ClassTag

object SortPlan {

  type Fn[D, E] = FetchedRow[D] => E
}

case class SortPlan[D, E: Ordering: ClassTag](
    override val child: ExecutionPlan[D],
    byFn: SortPlan.Fn[D, E],
    ascending: Boolean,
    numPartitions: OptionMagnet[Int]
) extends UnaryPlan[D, D](child) {

  /**
    * will break SquashedRow into pieces to be sorted by evidence
    */

  override protected def prepare: SquashedRDD[D] = {

    val unsquashed = child.squashedRDD
      .flatMap { v =>
        val rows = v.withCtx(child.spooky).unSquash
        rows
      }

    scratchRDDs.persist(unsquashed)
    // this can save some recomputation at the expense of memory
    // see https://issues.apache.org/jira/browse/SPARK-1021

    val sorted = numPartitions.original match {

      case Some(v) =>
        unsquashed.sortBy(byFn, ascending, v)
      case None =>
        unsquashed.sortBy(byFn, ascending)
    }

    val result = sorted.map { v =>
      SquashedRow(v.agentState.group, Seq(v.data))
    }

    result
  }
}
