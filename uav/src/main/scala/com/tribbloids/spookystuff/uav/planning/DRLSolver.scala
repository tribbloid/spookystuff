package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.dsl.GenPartitioners
import org.apache.spark.rdd.RDD

// TODO: all adaptive improvements goes here
case object DRLSolver extends MinimaxSolver {

  override def rewrite[V](
                         gp: GenPartitioners.MinimaxCost,
                         schema: DataRowSchema,
                         rdd: RDD[(TraceView, Iterable[V])]
                       ) = ???
}
