package com.tribbloids.spookystuff.uav.planning.CollisionAvoidances

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.planning.Constraints
import com.tribbloids.spookystuff.uav.planning.{CollisionAvoidance, Constraint, Resampler}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class Clearance(
                      traffic: Double = 1.0,
                      terrain: Option[Double] = None, //TODO: enable later
                      resampler: Option[Resampler] = None,
                      constraint: Option[Constraint] = Some(Constraints.AltitudeOnly)
                    ) extends CollisionAvoidance {

  override def rewrite[V: ClassTag](
                                     rdd: RDD[(TraceView, V)],
                                     schema: DataRowSchema
                                   ): RDD[(TraceView, V)] = {

    schema.ec.scratchRDDs.persist(rdd)

    val traces = rdd.keys.collect()




    //      val
    //
    //
    //
    //
    //
    //      val wpWithIDRDD: RDD[(List[(Waypoint, Int)], Long)] = traceWithIDRDD.map {
    //        tuple =>
    //          val wps: List[(Waypoint, Int)] = tuple._1
    //            .collect {
    //              case (k: Waypoint, v) => k -> v
    //            }
    //          wps -> tuple._2
    //      }
    //
    //      val home = spooky.getConf[UAVConf].home
    //
    //      val nedWithIDRDD: RDD[(List[(NED.V, Int)], Long)] = wpWithIDRDD.map {
    //        tuple =>
    //          val coords = tuple._1.flatMap {
    //            case (wp, i) =>
    //              wp._end.getCoordinate(NED, home).map {
    //                v =>
    //                  v -> i
    //              }
    //          }
    //          coords -> tuple._2
    //      }
    //
    //      val wpWithIDs = wpWithIDRDD.collect()
    //      val nedWithIDs = nedWithIDRDD.collect()




    ???
  }
}
