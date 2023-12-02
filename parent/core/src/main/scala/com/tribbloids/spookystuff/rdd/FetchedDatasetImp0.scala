package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.execution.ExecutionPlan

import scala.language.implicitConversions

trait FetchedDatasetImp0 {

  implicit def asPlan(self: FetchedDataset): ExecutionPlan = self.plan
}
