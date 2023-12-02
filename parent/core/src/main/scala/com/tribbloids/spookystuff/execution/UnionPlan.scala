package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.BottleneckRDD
import org.apache.spark.rdd.UnionRDD

case class UnionPlan(
    override val children: Seq[ExecutionPlan]
) extends ExecutionPlan(children) {

  // TODO: also use PartitionerAwareUnionRDD
  def doExecute(): BottleneckRDD = {
    new UnionRDD(
      spooky.sparkContext,
      children.map(_.bottleneckRDD)
    )
  }
}
