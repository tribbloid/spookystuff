package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.row.{BottleneckRDD, BottleneckRow, DataRow}
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 27/03/16.
  */
case class FetchPlan(
    override val child: ExecutionPlan,
    traces: TraceSet,
    keyBy: List[Action] => Any,
    genPartitioner: GenPartitioner
) extends UnaryPlan(child)
    with InjectBeaconRDDPlan {

  override def doExecute(): BottleneckRDD = {

    val trace_DataRowRDD: RDD[(Trace, DataRow)] = child.bottleneckRDD
      .flatMap {
        _.interpolateAndRewrite(traces)
      }
      .map {
        case (k, v) =>
          k.setSamenessFn(keyBy) -> v
      }

    val grouped = gpImpl.groupByKey(trace_DataRowRDD, beaconRDDOpt)

    grouped
      .map { tuple =>
        BottleneckRow(tuple._2.toVector, tuple._1) // actual fetch can only be triggered by extract or savePages
      }
  }
}
