package com.tribbloids.spookystuff.uav.planning.CollisionAvoidances

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitionerLike.Instance
import com.tribbloids.spookystuff.dsl.{GenPartitioner, GenPartitionerLike}
import com.tribbloids.spookystuff.row.{BeaconRDD, DataRowSchema}
import com.tribbloids.spookystuff.uav.planning._
import org.apache.spark.TaskContext
import org.apache.spark.mllib.uav.ClearanceSGDRunner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class Clearance(
                      traffic: Double = 1.0,
                      terrain: Option[Double] = None, //TODO: enable later
                      resampler: Option[Resampler] = None,
                      constraint: Option[Constraint] = Some(Constraints.AltitudeOnly)
                    ) extends GenPartitioner {

  override def getInstance[K >: TraceView <: TraceView : ClassTag](schema: DataRowSchema) =
    Inst(schema).asInstanceOf[Instance[K]]

  case class Inst(schema: DataRowSchema)(
    implicit val ctg: ClassTag[TraceView]) extends GenPartitionerLike.Instance[TraceView]{

    override def reduceByKey[V: ClassTag](
                                           rdd: RDD[(TraceView, V)],
                                           reducer: (V, V) => V,
                                           beaconRDDOpt: Option[BeaconRDD[TraceView]]
                                         ): RDD[(TraceView, V)] = {

      schema.ec.scratchRDDs.persist(rdd)
      val keyRDD = rdd.keys

      val pid2TracesRDD: RDD[(Int, List[TraceView])] = keyRDD
        .mapPartitions {
          itr =>
            val result: (Int, List[TraceView]) = TaskContext.get().partitionId() -> itr.toList
            Iterator(result)
        }

      val runner = ClearanceSGDRunner(pid2TracesRDD, schema, Clearance.this)
      val conversionMap_broadcast = runner.conversionMap_broadcast
      val result = rdd.map {
        case (trace, v) =>
          conversionMap_broadcast.value(trace) -> v
      }

      result
    }
  }
}
