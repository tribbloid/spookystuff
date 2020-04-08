package com.tribbloids.spookystuff.uav.planning.VRPOptimizers

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitionerLike.RepartitionKeyImpl
import com.tribbloids.spookystuff.row.{BeaconRDD, SpookySchema}
import com.tribbloids.spookystuff.uav.dsl.GenPartitioners
import com.tribbloids.spookystuff.uav.planning._
import com.tribbloids.spookystuff.uav.telemetry.{LinkStatus, LinkUtils}
import org.apache.spark.rdd.RDD

object JSprit extends VRPOptimizer {}

case class JSprit(
    problem: GenPartitioners.VRP,
    schema: SpookySchema
) extends RepartitionKeyImpl[TraceView] {

  override def repartitionKey(
      rdd: RDD[TraceView],
      beaconRDDOpt: Option[BeaconRDD[TraceView]]
  ): RDD[(TraceView, TraceView)] = {

    val spooky = schema.ec.spooky
    val tryLinkRDD = LinkUtils.tryLinkRDD(spooky, Some(rdd.partitions.length))
    val linkRDD = tryLinkRDD.flatMap(_.toOption)
    linkRDD.persist()

    val allUAVs = linkRDD.map(v => v.status()).collect()
    val uavs = problem.numUAVOverride match {
      case Some(n) => allUAVs.slice(0, n)
      case None    => allUAVs
    }

    val rows = rdd.collect()
    val solver = JSpritRunner(problem, schema, uavs, rows)

    val uav2TraceMap: Map[LinkStatus, Seq[TraceView]] = solver.getUAV2TraceMap
    val uav2TraceMap_broadcast = uav2TraceMap

    val old2NewTraceRDD: RDD[(TraceView, TraceView)] = linkRDD.flatMap { link =>
      val status = link.status()
      val traces: Seq[TraceView] = uav2TraceMap_broadcast.value.getOrElse(status, Nil)
      val trace2WithUAV: Seq[(TraceView, TraceView)] = traces.map { trace =>
        val withUAV = trace.copy(
          children = List(PreferUAV(status, link._lock._id))
            ++ trace.children
        )
        val rewrittenOpt = withUAV.rewriteLocally(schema)
        trace -> TraceView(rewrittenOpt.getOrElse(Nil))
      }

      trace2WithUAV
    }
    old2NewTraceRDD
  }
}
