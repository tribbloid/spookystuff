package com.tribbloids.spookystuff.uav.planning.minimax

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.dsl.GenPartitioners
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

// TODO: all adaptive improvements goes here
case object DRLSolver extends MinimaxSolver {

  override def solve[V: ClassTag](
                                   gp: GenPartitioners.MinimaxCost,
                                   schema: DataRowSchema,
                                   rdd: RDD[(TraceView, V)]
                                 ) = ???
}
