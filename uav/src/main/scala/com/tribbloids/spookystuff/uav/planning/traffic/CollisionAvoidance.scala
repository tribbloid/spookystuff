package com.tribbloids.spookystuff.uav.planning.traffic

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.row.DataRowSchema
import org.apache.spark.rdd.RDD

trait CollisionAvoidance {

  def rewrite[V](
                  rdd: RDD[(TraceView, Iterable[V])],
                  schema: DataRowSchema
                ): RDD[(TraceView, Iterable[V])] = rdd
}