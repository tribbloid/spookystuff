package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.row.{BeaconRDD, SpookySchema, SquashedFetchedRDD}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Basic Plan with no children, isExecuted always= true
  */
case class RDDPlan(
    sourceRDD: SquashedFetchedRDD,
    override val schema: SpookySchema,
    beaconRDD: Option[BeaconRDD[TraceView]] = None
) extends ExecutionPlan(
      Seq(),
      schema.ec
    ) {

  override lazy val beaconRDDOpt = beaconRDD

  override def doExecute(): SquashedFetchedRDD = sourceRDD
}
