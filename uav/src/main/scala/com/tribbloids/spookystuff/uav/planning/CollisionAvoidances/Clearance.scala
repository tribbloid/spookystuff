package com.tribbloids.spookystuff.uav.planning.CollisionAvoidances

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.planning.Constraints
import com.tribbloids.spookystuff.uav.planning.{CollisionAvoidance, Constraint, Resampler}
import org.apache.spark.mllib.uav.ClearanceRunner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class Clearance(
                      traffic: Double = 1.0,
                      terrain: Option[Double] = None, //TODO: enable later
                      resampler: Option[Resampler] = None,
                      constraint: Option[Constraint] = Some(Constraints.AltitudeOnly)
                    ) extends CollisionAvoidance {

  override def solve[V: ClassTag](
                                   rdd: RDD[(TraceView, V)],
                                   schema: DataRowSchema
                                 ): RDD[(TraceView, V)] = {

    val runner = ClearanceRunner(rdd, schema, this)
    runner.solved
  }
}
