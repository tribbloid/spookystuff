package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.{FetchOptimizer, FetchOptimizers}
import com.tribbloids.spookystuff.row.{DataRow, SquashedFetchedRDD, SquashedFetchedRow}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

trait InjectBeaconRDDPlan extends ExecutionPlan {

  def fetchOptimizer: FetchOptimizer
  def partitionerFactory: RDD[_] => Partitioner

  abstract override lazy val beaconRDDOpt: Option[RDD[(Trace, DataRow)]] = {
    fetchOptimizer match {
      case FetchOptimizers.WebCacheAware =>
        val inherited = super.defaultBeaconRDDOpt
        inherited.orElse{
          this.firstChildOpt.map {
            child =>
              spooky.createBeaconRDD[Trace, DataRow](child.rdd(), partitionerFactory)
          }
        }
      case _ =>
        super.defaultBeaconRDDOpt
    }
  }
}

/**
  * Created by peng on 27/03/16.
  */
case class FetchPlan(
                      override val child: ExecutionPlan,
                      traces: Set[Trace],
                      partitionerFactory: RDD[_] => Partitioner,
                      fetchOptimizer: FetchOptimizer
                    ) extends UnaryPlan(child) with InjectBeaconRDDPlan {

  import com.tribbloids.spookystuff.dsl._
  import com.tribbloids.spookystuff.utils.ImplicitUtils._

  override def doExecute(): SquashedFetchedRDD = {

    val trace_DataRowRDD: RDD[(Trace, DataRow)] = child.rdd()
      .flatMap {
        _.interpolate(traces)
      }

    val partitioner = partitionerFactory(trace_DataRowRDD)
    val grouped: RDD[(Trace, Iterable[DataRow])] =
      fetchOptimizer match {
        case FetchOptimizers.Narrow =>
          trace_DataRowRDD.groupByKey_narrow()
        case FetchOptimizers.Wide =>
          trace_DataRowRDD.groupByKey(partitioner)
        case FetchOptimizers.WebCacheAware =>
          trace_DataRowRDD.groupByKey_beacon(beaconRDDOpt.get)
        case _ => throw new NotImplementedError(s"${fetchOptimizer.getClass.getSimpleName} optimizer is not supported")
      }

    grouped
      .map {
        tuple =>
          SquashedFetchedRow(tuple._2.toArray, TraceView(tuple._1)) // actual fetch can only be triggered by extract or savePages
      }
  }
}
