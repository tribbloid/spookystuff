package com.tribbloids.spookystuff.rdd

import com.tribbloids.spookystuff.execution.ExecutionPlan

import scala.language.implicitConversions

trait FetchedDatasetImp0 {

  implicit def asPlan[D](self: FetchedDataset[D]): ExecutionPlan[D] = self.plan
}
