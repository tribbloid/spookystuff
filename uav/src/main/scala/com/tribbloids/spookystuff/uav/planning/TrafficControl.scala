package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.execution.ExecutionContext
import org.apache.spark.rdd.RDD

trait TrafficControl {

  def rewrite[V](
                  ec: ExecutionContext,
                  rdd: RDD[(TraceView, Iterable[V])]
                ): RDD[(TraceView, Iterable[V])]
}

object TrafficControl {

  case object Disabled extends TrafficControl {

    override def rewrite[V](
                             ec: ExecutionContext,
                             rdd: RDD[(TraceView, Iterable[V])]
                           ): RDD[(TraceView, Iterable[V])] = rdd
  }

  //
  case class Clearance_AltitudeOnly(
                                     distance: Double
                                   ) extends TrafficControl {

    override def rewrite[V](
                             ec: ExecutionContext,
                             rdd: RDD[(TraceView, Iterable[V])]
                           ): RDD[(TraceView, Iterable[V])] = {

      val withIDs = rdd.zipWithUniqueId()
      ec.scratchRDDs.persist(withIDs)
      val traceViewWIDs = withIDs.map {
        tuple =>
          tuple._1._1 -> tuple._2
      }
        .collect()


      ???
    }
  }
}