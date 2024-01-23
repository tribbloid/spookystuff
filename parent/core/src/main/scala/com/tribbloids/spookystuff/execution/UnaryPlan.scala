package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.SpookySchema

abstract class UnaryPlan(
    val child: ExecutionPlan
) extends ExecutionPlan(Seq(child)) {}

object UnaryPlan {

  abstract class NoSchemaChange(
      override val child: ExecutionPlan
  ) extends UnaryPlan(child) {

    override def computeSchema: SpookySchema = child.outputSchema
  }
}
