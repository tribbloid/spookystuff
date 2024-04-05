package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.row._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object ForkPlan {

  type Out[O] = Seq[(TraceSet, O)]

  type Fn[I, O] = FetchedRow[I] => Out[O]
}

/**
  * Created by peng on 27/03/16.
  */
case class ForkPlan[I, O: ClassTag](
    override val child: ExecutionPlan[I],
    fn: ForkPlan.Fn[I, O],
    sameBy: Trace => Any,
    genPartitioner: GenPartitioner
) extends UnaryPlan[I, O](child)
    with InjectBeaconRDDPlan[O] {

  override def execute: SquashedRDD[O] = {

    val forkedRDD: RDD[(LocalityGroup, O)] = child.squashedRDD
      .flatMap { v: SquashedRow[I] =>
        val rows = v.withCtx(child.spooky).unSquash

        val seq = rows.flatMap(fn)

        seq.flatMap {
          case (ks, v) =>
            ks.map { trace =>
              LocalityGroup(trace)().sameBy(sameBy) -> v
            }
        }
      }

    val grouped = gpImpl.groupByKey(forkedRDD, beaconRDDOpt)

    grouped
      .map { tuple =>
        SquashedRow(
          AgentState(tuple._1),
          tuple._2.map { v =>
            Data.WithScope.empty(v)
          }.toVector
        )
          .withCtx(spooky)
          .withDefaultScope

        // actual fetch can only be triggered by extract or savePages
      }
  }
}
