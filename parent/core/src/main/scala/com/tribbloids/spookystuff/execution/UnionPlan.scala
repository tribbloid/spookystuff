package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.SquashedRDD
import org.apache.spark.rdd.UnionRDD

case class UnionPlan[D](
    override val children: Seq[ExecutionPlan[D]]
) extends ExecutionPlan[D](children) {

  // TODO: also use PartitionerAwareUnionRDD
  def prepare: SquashedRDD[D] = {
    new UnionRDD(
      spooky.sparkContext,
      children.map(_.squashedRDD)
    )
  }
}
