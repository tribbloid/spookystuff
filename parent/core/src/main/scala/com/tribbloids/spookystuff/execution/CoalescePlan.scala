package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.{SquashedRDD, SquashedRow}
import org.apache.spark.rdd.RDD

case class CoalescePlan[D](
    override val child: ExecutionPlan[D],
    numPartitions: RDD[?] => Int,
    shuffle: Boolean = false,
    ord: Ordering[SquashedRow[D]] = null
) extends UnaryPlan[D, D](child) {

  override protected def prepare: SquashedRDD[D] = {

    val childRDD = child.squashedRDD
    val n = numPartitions(childRDD)
    childRDD.coalesce(n, shuffle)(ord)
  }
}
