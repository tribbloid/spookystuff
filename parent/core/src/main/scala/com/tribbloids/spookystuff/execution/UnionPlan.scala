package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.extractors.DataType
import com.tribbloids.spookystuff.row.{Field, SpookySchema, SquashedRDD}
import org.apache.spark.rdd.UnionRDD

import scala.collection.immutable.ListMap

case class UnionPlan(
    override val children: Seq[ExecutionPlan]
) extends ExecutionPlan(children) {

  // TODO: also use PartitionerAwareUnionRDD
  def execute: SquashedRDD = {
    new UnionRDD(
      spooky.sparkContext,
      children.map(_.squashedRDD)
    )
  }

  override def computeSchema: SpookySchema = {
    SpookySchema(
      ec,
      fieldTypes = children
        .map(_.outputSchema.fieldTypes)
        .reduceOption(_ ++ _)
        .getOrElse(ListMap.empty[Field, DataType])
    )
  }
}
