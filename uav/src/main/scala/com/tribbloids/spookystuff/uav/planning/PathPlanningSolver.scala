package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.row.DataRowSchema
import org.apache.spark.rdd.RDD

trait PathPlanningSolver[T <: GenPartitioner] {

  def rewrite[V](
                  gp: T,
                  schema: DataRowSchema,
                  rdd: RDD[(TraceView, Iterable[V])]
                ): RDD[(TraceView, Iterable[V])]
}
