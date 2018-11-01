package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.row.{BeaconRDD, SpookySchema, SquashedFetchedRDD}

/**
  * Basic Plan with no children, isExecuted always= true
  */
case class RDDPlan(
    override val schema: SpookySchema,
    @transient sourceRDD: SquashedFetchedRDD,
    @transient beaconRDD: Option[BeaconRDD[TraceView]] = None
) extends ExecutionPlan(
      Seq(),
      schema.ec
    ) {

  override lazy val beaconRDDOpt = beaconRDD

  override def doExecute(): SquashedFetchedRDD = sourceRDD
}
