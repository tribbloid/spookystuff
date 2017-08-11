package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.uav.dsl.GenPartitioners
import org.apache.spark.rdd.RDD

trait MinimaxSolver {

  def getRealignedRDD[V](
                          minimax: GenPartitioners.MinimaxCost,
                          spooky: SpookyContext,
                          rowRDD: RDD[(TraceView, Iterable[V])]
                        ): RDD[(TraceView, Iterable[V])]
}
