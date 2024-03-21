package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.SpookySchema

abstract class UnaryPlan[D](
    val child: ExecutionPlan[_]
) extends ExecutionPlan(Seq(child)) {}

object UnaryPlan {

  abstract class NoSchemaChange[D](
      override val child: ExecutionPlan[D]
  ) extends UnaryPlan(child) {

    override def computeSchema: SpookySchema[D] = child.outputSchema
  }
}
