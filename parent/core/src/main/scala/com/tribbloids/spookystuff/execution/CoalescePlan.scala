package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.{BottleneckRDD, BottleneckRow}
import org.apache.spark.rdd.RDD

case class CoalescePlan(
    override val child: ExecutionPlan,
    numPartitions: RDD[_] => Int,
    shuffle: Boolean = false,
    ord: Ordering[BottleneckRow] = null
) extends UnaryPlan(child) {

  def doExecute(): BottleneckRDD = {
    val childRDD = child.bottleneckRDD
    val n = numPartitions(childRDD)
    childRDD.coalesce(n, shuffle)(ord)
  }
}
