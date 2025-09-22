package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.execution.ExecutionPlan

import scala.language.implicitConversions

trait DavaView_Imp0 {

  implicit def asPlan[D](self: DataView[D]): ExecutionPlan[D] = self.plan
}
