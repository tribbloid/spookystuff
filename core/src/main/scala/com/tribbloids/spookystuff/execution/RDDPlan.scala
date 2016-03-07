package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.row.{DataRow, Field, SquashedRowRDD}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer

/**
  * Basic Plan with no children, isExecuted always= true
  */
case class RDDPlan(
                    sourceRDD: SquashedRowRDD,
                    override val fieldSet: ListSet[Field],
                    override val spooky: SpookyContext,
                    localityBeaconRDD: Option[RDD[(Trace, DataRow)]] = None,
                    override val cacheQueue: ArrayBuffer[RDD[_]] = ArrayBuffer()
                  ) extends AbstractExecutionPlan(Seq(), fieldSet, spooky, cacheQueue) {

  override lazy val localityBeaconRDDOpt = localityBeaconRDD

  override def doExecute(): SquashedRowRDD = sourceRDD
}
