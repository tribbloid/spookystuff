package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.row.{BeaconRDD, DataRow, SquashedFetchedRDD, SquashedFetchedRow}
import org.apache.spark.rdd.RDD

trait InjectBeaconRDDPlan extends ExecutionPlan {

  def genPartitioner: GenPartitioner

  abstract override lazy val beaconRDDOpt: Option[BeaconRDD[TraceView]] = {
    inheritedBeaconRDDOpt.orElse {
      this.firstChildOpt.flatMap {
        child =>
          genPartitioner.getImpl.createBeaconRDD[TraceView](child.rdd())
      }
    }
  }
}

/**
  * Created by peng on 27/03/16.
  */
case class FetchPlan(
                      override val child: ExecutionPlan,
                      traces: Set[Trace],
                      genPartitioner: GenPartitioner
                    ) extends UnaryPlan(child) with InjectBeaconRDDPlan {

  override def doExecute(): SquashedFetchedRDD = {

    val trace_DataRowRDD: RDD[(TraceView, DataRow)] = child.rdd()
      .flatMap {
        _.interpolate(traces)
      }

    val gpImpl = genPartitioner.getImpl
    val beaconRDDOpt = this.beaconRDDOpt
    val grouped = gpImpl.groupByKey(trace_DataRowRDD, beaconRDDOpt)

    grouped
      .map {
        tuple =>
          SquashedFetchedRow(tuple._2.toArray, tuple._1) // actual fetch can only be triggered by extract or savePages
      }
  }
}
