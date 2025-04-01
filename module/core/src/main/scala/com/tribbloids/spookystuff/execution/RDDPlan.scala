package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.row.{BeaconRDD, LocalityGroup, SpookySchema, SquashedRDD}

/**
  * Basic Plan with no children, isExecuted always= true
  */
case class RDDPlan[D](
    override val computeSchema: SpookySchema,
    @transient prepare: SquashedRDD[D],
    @transient beaconRDD: Option[BeaconRDD[LocalityGroup]] = None
) extends ExecutionPlan[D](
      Seq(),
      computeSchema.ec
    ) {

  override lazy val beaconRDDOpt: Option[BeaconRDD[LocalityGroup]] = beaconRDD
}
