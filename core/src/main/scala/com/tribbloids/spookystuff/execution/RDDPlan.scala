package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.row.{DataRow, Field, DataRowSchema, SquashedFetchedRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer

/**
  * Basic Plan with no children, isExecuted always= true
  */
case class RDDPlan(
                    sourceRDD: SquashedFetchedRDD,
                    override val schema: DataRowSchema,
                    override val spooky: SpookyContext,
                    beaconRDD: Option[RDD[(Trace, DataRow)]] = None,
                    override val cacheQueue: ArrayBuffer[RDD[_]] = ArrayBuffer()
                  ) extends ExecutionPlan(Seq(), spooky, cacheQueue) {

  override lazy val beaconRDDOpt = beaconRDD

  override def doExecute(): SquashedFetchedRDD = sourceRDD
}
