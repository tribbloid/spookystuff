package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.{GenPartitioner, GenPartitionerLike}
import com.tribbloids.spookystuff.row.BeaconRDD

trait InjectBeaconRDDPlan extends ExecutionPlan {

  def genPartitioner: GenPartitioner

  lazy val gpImpl: GenPartitionerLike.Instance[Trace] = {
    genPartitioner.getInstance[Trace](schema)
  }

  abstract override lazy val beaconRDDOpt: Option[BeaconRDD[Trace]] = {
    inheritedBeaconRDDOpt.orElse {
      this.firstChildOpt.flatMap { child =>
        val beaconRDDOpt = gpImpl.createBeaconRDD(child.bottleneckRDD)
        beaconRDDOpt
      }
    }
  }
}
