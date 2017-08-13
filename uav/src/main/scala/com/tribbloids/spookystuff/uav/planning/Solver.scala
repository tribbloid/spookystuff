package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.execution.ExecutionContext
import org.apache.spark.rdd.RDD

trait Solver[T <: GenPartitioner] {

  def rewrite[V](
                  gp: T,
                  ec: ExecutionContext,
                  rdd: RDD[(TraceView, Iterable[V])]
                ): RDD[(TraceView, Iterable[V])]
}
