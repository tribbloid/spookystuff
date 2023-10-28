package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.row.{DataRow, SquashedFetchedRDD, SquashedRow}
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 27/03/16.
  */
case class FetchPlan(
    override val child: ExecutionPlan,
    traces: Set[Trace],
    keyBy: Trace => Any,
    genPartitioner: GenPartitioner
) extends UnaryPlan(child)
    with InjectBeaconRDDPlan {

  override def doExecute(): SquashedFetchedRDD = {

    val trace_DataRowRDD: RDD[(TraceView, DataRow)] = child
      .rdd()
      .flatMap {
        _.interpolateAndRewriteLocally(traces)
      }
      .map {
        case (k, v) =>
          k.keyBy(keyBy) -> v
      }

    val grouped = gpImpl.groupByKey(trace_DataRowRDD, beaconRDDOpt)

    grouped
      .map { tuple =>
        SquashedRow(tuple._2.toArray, tuple._1) // actual fetch can only be triggered by extract or savePages
      }
  }
}
