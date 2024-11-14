package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.execution.ExecutionPlan

import scala.language.implicitConversions

trait SpookyDataset_Imp0 {

  implicit def asPlan[D](self: SpookyDataset[D]): ExecutionPlan[D] = self.plan
}
