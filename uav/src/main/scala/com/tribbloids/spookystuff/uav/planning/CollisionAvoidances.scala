package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.spatial.NED
import org.apache.spark.rdd.RDD

object CollisionAvoidances {

  case object None extends CollisionAvoidance {

    override def rewrite[V](
                             ec: ExecutionContext,
                             rdd: RDD[(TraceView, Iterable[V])]
                           ): RDD[(TraceView, Iterable[V])] = rdd
  }

  sealed case class AltitudeOnly(
                                  trafficClearance: Option[Double] = Some(1),
                                  terrainClearance: Option[Double] = Some(1)
                                ) extends CollisionAvoidance {

    override def rewrite[V](
                             ec: ExecutionContext,
                             rdd: RDD[(TraceView, Iterable[V])]
                           ): RDD[(TraceView, Iterable[V])] = {

      import ec._

      val withIDs = rdd.zipWithUniqueId()
      val traceWithIDRDD = withIDs.map {
        tuple =>
          tuple._1._1.children.zipWithIndex -> tuple._2
      }
      ec.scratchRDDs.persist(traceWithIDRDD)

      val wpWithIDRDD: RDD[(List[(Waypoint, Int)], Long)] = traceWithIDRDD.map {
        tuple =>
          val wps: List[(Waypoint, Int)] = tuple._1
            .collect {
              case (k: Waypoint, v) => k -> v
            }
          wps -> tuple._2
      }

      val home = spooky.getConf[UAVConf].home

      val nedWithIDRDD: RDD[(List[(NED.V, Int)], Long)] = wpWithIDRDD.map {
        tuple =>
          val coords = tuple._1.flatMap {
            case (wp, i) =>
              wp._end.getCoordinate(NED, home).map {
                v =>
                  v -> i
              }
          }
          coords -> tuple._2
      }

      val wpWithIDs = wpWithIDRDD.collect()
      val nedWithIDs = nedWithIDRDD.collect()




      ???
    }
  }
}
