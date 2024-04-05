package com.tribbloids.spookystuff.execution

abstract class UnaryPlan[I, O](
    val child: ExecutionPlan[I]
) extends ExecutionPlan[O](Seq(child)) {}

object UnaryPlan {

//  abstract class NoSchemaChange[D](
//      override val child: ExecutionPlan[D]
//  ) extends UnaryPlan(child) {}
}
