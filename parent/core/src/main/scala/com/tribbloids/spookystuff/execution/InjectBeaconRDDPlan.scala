package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.dsl.{GenPartitioner, GenPartitionerLike}
import com.tribbloids.spookystuff.row.{BeaconRDD, LocalityGroup}

trait InjectBeaconRDDPlan extends ExecutionPlan {

  def genPartitioner: GenPartitioner

  lazy val gpImpl: GenPartitionerLike.Instance[LocalityGroup] = {
    genPartitioner.getInstance[LocalityGroup](outputSchema)
  }

  abstract override lazy val beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = {
    inheritedBeaconRDDOpt.orElse {
      this.firstChildOpt.flatMap { child =>
        val beaconRDDOpt = gpImpl.createBeaconRDD(child.squashedRDD)

        beaconRDDOpt
      }
    }
  }
}
