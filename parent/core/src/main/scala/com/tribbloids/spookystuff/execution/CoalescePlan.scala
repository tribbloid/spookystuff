package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.{SpookySchema, SquashedRDD, SquashedRow}
import org.apache.spark.rdd.RDD

case class CoalescePlan[D](
    override val child: ExecutionPlan[D],
    numPartitions: RDD[_] => Int,
    shuffle: Boolean = false,
    ord: Ordering[SquashedRow[D]] = null
) extends UnaryPlan[D, D](child) {

//  override def execute(schema: SpookySchema[D]): SquashedRDD[D] = {
//    val childRDD = child.squashedRDD
//    val n = numPartitions(childRDD)
//    childRDD.coalesce(n, shuffle)(ord)
//  }

  override protected def execute: SquashedRDD[D] = {

    val childRDD = child.squashedRDD
    val n = numPartitions(childRDD)
    childRDD.coalesce(n, shuffle)(ord)
  }
}
