package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.{FetchOptimizer, FetchOptimizers}
import com.tribbloids.spookystuff.row.{DataRow, SquashedPageRow, SquashedRowRDD}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

trait CreateOrInheritBeaconRDDPlan extends AbstractExecutionPlan {

  def fetchOptimizer: FetchOptimizer
  def partitionerFactory: RDD[_] => Partitioner

  abstract override lazy val localityBeaconRDDOpt: Option[RDD[(Trace, DataRow)]] = {
    fetchOptimizer match {
      case FetchOptimizers.WebCacheAware =>
        val inherited = super.defaultLocalityBeaconRDDOpt
        inherited.orElse{
          this.firstChildOpt.map {
            child =>
              spooky.createBeaconRDD[Trace, DataRow](child.rdd(), partitionerFactory)
          }
        }
      case _ =>
        super.defaultLocalityBeaconRDDOpt
    }
  }
}

/**
  * Created by peng on 27/03/16.
  */
case class FetchPlan(
                      child: AbstractExecutionPlan,
                      traces: Set[Trace],
                      partitionerFactory: RDD[_] => Partitioner,
                      fetchOptimizer: FetchOptimizer
                    ) extends AbstractExecutionPlan(child) with CreateOrInheritBeaconRDDPlan {

  import com.tribbloids.spookystuff.dsl._
  import com.tribbloids.spookystuff.utils.Views._

  override def doExecute(): SquashedRowRDD = {

    val trace_DataRowRDD: RDD[(Trace, DataRow)] = child.rdd()
      .flatMap {
        _.interpolate(traces, spooky)
      }

    val partitioner = partitionerFactory(trace_DataRowRDD)
    val grouped: RDD[(Trace, Iterable[DataRow])] =
      fetchOptimizer match {
        case FetchOptimizers.Narrow =>
          trace_DataRowRDD.groupByKey_narrow()
        case FetchOptimizers.Wide =>
          trace_DataRowRDD.groupByKey(partitioner)
        case FetchOptimizers.WebCacheAware =>
          trace_DataRowRDD.groupByKey_beacon(localityBeaconRDDOpt.get)
        case _ => throw new NotImplementedError(s"${fetchOptimizer.getClass.getSimpleName} optimizer is not supported")
      }

    grouped
      .map {
        tuple =>
          SquashedPageRow(tuple._2.toArray, tuple._1) // actual fetch can only be triggered by extract or savePages
      }
  }
}
