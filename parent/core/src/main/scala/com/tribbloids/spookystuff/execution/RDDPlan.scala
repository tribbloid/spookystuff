package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.row.{BeaconRDD, BottleneckRDD, SpookySchema}

/**
  * Basic Plan with no children, isExecuted always= true
  */
case class RDDPlan(
    override val schema: SpookySchema,
    @transient sourceRDD: BottleneckRDD,
    @transient beaconRDD: Option[BeaconRDD[Trace]] = None
) extends ExecutionPlan(
      Seq(),
      schema.ec
    ) {

  override lazy val beaconRDDOpt: Option[BeaconRDD[Trace]] = beaconRDD

  override def doExecute(): BottleneckRDD = sourceRDD
}
