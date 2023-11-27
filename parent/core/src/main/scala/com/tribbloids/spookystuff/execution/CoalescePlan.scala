package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.{SquashedRDD, SquashedRow}
import org.apache.spark.rdd.RDD

case class CoalescePlan(
    override val child: ExecutionPlan,
    numPartitions: RDD[_] => Int,
    shuffle: Boolean = false,
    ord: Ordering[SquashedRow] = null
) extends UnaryPlan.NoSchemaChange(child) {

  def execute: SquashedRDD = {
    val childRDD = child.squashedRDD
    val n = numPartitions(childRDD)
    childRDD.coalesce(n, shuffle)(ord)
  }
}
