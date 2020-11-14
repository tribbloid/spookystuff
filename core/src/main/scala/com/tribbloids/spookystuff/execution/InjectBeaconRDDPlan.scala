package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.{GenPartitioner, GenPartitionerLike}
import com.tribbloids.spookystuff.row.{BeaconRDD, DataRow, SquashedFetchedRDD, SquashedFetchedRow}
import org.apache.spark.rdd.RDD

trait InjectBeaconRDDPlan extends ExecutionPlan {

  def genPartitioner: GenPartitioner

  lazy val gpImpl: GenPartitionerLike.Instance[TraceView] = {
    genPartitioner.getInstance[TraceView](schema)
  }

  abstract override lazy val beaconRDDOpt: Option[BeaconRDD[TraceView]] = {
    inheritedBeaconRDDOpt.orElse {
      this.firstChildOpt.flatMap { child =>
        val beaconRDDOpt = gpImpl.createBeaconRDD(child.rdd())
        beaconRDDOpt
      }
    }
  }
}
