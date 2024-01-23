package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.row.{AgentState, DataRow, LocalityGroup, SquashedRDD, SquashedRow}
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 27/03/16.
  */
case class FetchPlan(
    override val child: ExecutionPlan,
    traces: TraceSet,
    sameBy: Trace => Any,
    genPartitioner: GenPartitioner
) extends UnaryPlan.NoSchemaChange(child)
    with InjectBeaconRDDPlan {

  override def execute: SquashedRDD = {

    val trace_DataRowRDD: RDD[(LocalityGroup, DataRow)] = child.squashedRDD
      .flatMap { v =>
        v.withSchema(outputSchema).interpolateAndRewrite(traces)
      }
      .map {
        case (k, v) =>
          LocalityGroup(k)().sameBy(sameBy) -> v
      }

    val grouped = gpImpl.groupByKey(trace_DataRowRDD, beaconRDDOpt)

    grouped
      .map { tuple =>
        SquashedRow(
          AgentState(tuple._1),
          tuple._2.map(_.withEmptyScope).toVector
        )
          .withCtx(spooky)
          .resetScope

        // actual fetch can only be triggered by extract or savePages
      }
  }
}
