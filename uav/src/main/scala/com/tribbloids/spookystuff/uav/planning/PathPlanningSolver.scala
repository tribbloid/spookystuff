package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.row.DataRowSchema
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait PathPlanningSolver[T <: GenPartitioner] {

  def solve[V: ClassTag](
                          gp: T,
                          schema: DataRowSchema,
                          rdd: RDD[(TraceView, V)]
                        ): RDD[(TraceView, V)]
}
