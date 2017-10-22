package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.row.DataRowSchema
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait CollisionAvoidance extends Serializable {

  def rewrite[V: ClassTag](
                            rdd: RDD[(TraceView, V)],
                            schema: DataRowSchema
                          ): RDD[(TraceView, V)] = rdd
}
