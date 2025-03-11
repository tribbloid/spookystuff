package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.dsl.{Locality, DataLocality}
import com.tribbloids.spookystuff.row.{BeaconRDD, LocalityGroup}

trait CanInjectBeaconRDD[O] extends ExecutionPlan[O] {

  def locality: Locality

  lazy val gpImpl: DataLocality.Instance[LocalityGroup] = {
    locality.getInstance[LocalityGroup](outputSchema)
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
