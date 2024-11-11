package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.dsl.{GenPartitioner, GenPartitionerLike}
import com.tribbloids.spookystuff.row.{BeaconRDD, LocalityGroup}

trait CanInjectBeaconRDD[O] extends ExecutionPlan[O] {

  def genPartitioner: GenPartitioner

  lazy val gpImpl: GenPartitionerLike.Instance[LocalityGroup] = {
    genPartitioner.getInstance[LocalityGroup](outputSchema)
  }

  final override lazy val beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = {
    inheritedBeaconRDDOpt.orElse {
      this.firstChildOpt.flatMap { child =>
        val beaconRDDOpt = gpImpl.createBeaconRDD(child.squashedRDD)

        beaconRDDOpt
      }
    }
  }
}
