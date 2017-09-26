package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.spatial.NED
import org.apache.spark.rdd.RDD

trait CollisionAvoidance {

  def rewrite[V](
                  schema: DataRowSchema,
                  rdd: RDD[(TraceView, Iterable[V])]
                ): RDD[(TraceView, Iterable[V])]
}

object CollisionAvoidance {

  def t4MinimalDist(
                     A1: NED.V,
                     B1: NED.V,
                     A2: NED.V,
                     B2: NED.V
                   ): (Double, Double) = {

    val M = A1.vector - A2.vector
    val C1 = B1.vector - A1.vector
    val C2 = B2.vector - A2.vector

    val C21 = C2 * C1.t
    val G = C21 - C21.t

    val C1TGC2 = C1.t * G * C2

    val _t1 = - (M.t * G * C2) / C1TGC2
    val _t2 = - (M.t * G * C1) / C1TGC2

    val t1 = Math.max(Math.min(1.0, _t1), 0.0)
    val t2 = Math.max(Math.min(1.0, _t2), 0.0)

    t1 -> t2
  }
}
