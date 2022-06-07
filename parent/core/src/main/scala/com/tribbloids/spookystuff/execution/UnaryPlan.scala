package com.tribbloids.spookystuff.execution

abstract class UnaryPlan(
    val child: ExecutionPlan
) extends ExecutionPlan(Seq(child)) {}
