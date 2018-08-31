package com.tribbloids.spookystuff.uav.planning.TrafficControls

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitionerLike
import com.tribbloids.spookystuff.dsl.GenPartitionerLike.Instance
import com.tribbloids.spookystuff.row.{BeaconRDD, SpookySchema}
import com.tribbloids.spookystuff.uav.planning._
import org.apache.spark.TaskContext
import org.apache.spark.ml.uav.AvoidSGDRunner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class Avoid(
    traffic: Option[Double] = Some(1.0),
    terrain: Option[Double] = None, //TODO: enable later
    resampler: Option[Resampler] = None,
    constraint: Option[Constraint] = Some(Constraints.AltitudeOnly)
) extends TrafficControl {

  val _traffic = traffic.getOrElse(0.0)

  override def getInstance[K >: TraceView <: TraceView: ClassTag](schema: SpookySchema) =
    Inst(schema).asInstanceOf[Instance[K]]

  case class Inst(schema: SpookySchema)(implicit val ctg: ClassTag[TraceView])
      extends GenPartitionerLike.Instance[TraceView] {

    override def reduceByKey[V: ClassTag](
        rdd: RDD[(TraceView, V)],
        reducer: (V, V) => V,
        beaconRDDOpt: Option[BeaconRDD[TraceView]]
    ): RDD[(TraceView, V)] = {

      schema.ec.persist(rdd)
      val keyRDD = rdd.keys

      val pid2TracesRDD: RDD[(Int, List[TraceView])] = keyRDD
        .mapPartitions { itr =>
          val result: (Int, List[TraceView]) = TaskContext.get().partitionId() -> itr.toList
          Iterator(result)
        }

      val runner = AvoidSGDRunner(pid2TracesRDD, schema, Avoid.this) //TODO: not general enough for distributed runner
      val conversionMap_broadcast = runner.conversionMap_broadcast
      val result = rdd.map {
        case (trace, v) =>
          TraceView(conversionMap_broadcast.value(trace.children)) -> v
      }

      result
    }
  }
}
