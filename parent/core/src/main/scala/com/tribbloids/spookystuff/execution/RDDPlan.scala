package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.{BeaconRDD, LocalityGroup, SpookySchema, SquashedRDD}

/**
  * Basic Plan with no children, isExecuted always= true
  */
case class RDDPlan(
    override val computeSchema: SpookySchema,
    @transient execute: SquashedRDD,
    @transient beaconRDD: Option[BeaconRDD[LocalityGroup]] = None
) extends ExecutionPlan(
      Seq(),
      computeSchema.ec
    ) {

  override lazy val beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = beaconRDD
}
