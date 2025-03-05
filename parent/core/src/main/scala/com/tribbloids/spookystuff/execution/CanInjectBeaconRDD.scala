package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.dsl.{Locality, LocalityLike}
import com.tribbloids.spookystuff.row.{BeaconRDD, LocalityGroup}

trait CanInjectBeaconRDD[O] extends ExecutionPlan[O] {

  def genPartitioner: Locality

  lazy val gpImpl: LocalityLike.Instance[LocalityGroup] = {
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
