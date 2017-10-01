package com.tribbloids.spookystuff.uav.planning.traffic

import com.tribbloids.spookystuff.actions.{Rewriter, Trace, TraceView}
import com.tribbloids.spookystuff.row.DataRowSchema
import org.apache.spark.mllib.optimization.{DVec, Vec}
import org.apache.spark.rdd.RDD

object Clearance {

  type Interpolation = Rewriter[Trace]
  type LocatioUpdater = Rewriter[Vec]

  object NoInperolation extends Interpolation

  object AltitudeOnly extends LocatioUpdater {
    override def rewrite(v: Vec, schema: DataRowSchema): Vec = {
      val alt = v(3)
      new DVec(Array(0,0,alt))
    }
  }
}

case class Clearance(
                      traffic: Double = 1,
                      terrain: Option[Double] = None, //TODO: enable later
                      interpolation: Clearance.Interpolation,
                      locationUpdater: Clearance.LocatioUpdater
                    ) extends CollisionAvoidance {

  override def rewrite[V](
                           rdd: RDD[(TraceView, Iterable[V])],
                           schema: DataRowSchema
                         ): RDD[(TraceView, Iterable[V])] = {

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
