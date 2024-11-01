package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl.{GenPartitioner, OneToMany}
import com.tribbloids.spookystuff.row._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object FetchPlan {

  type Batch[O] = Seq[(TraceSet, O)]

  type Fn[I, O] = FetchedRow[I] => Batch[O]

  object TraceOnly {

    type _Fn[I] = FetchedRow[I] => Seq[TraceSet]

    def normalise[I](
        fn: _Fn[I],
        forkType: OneToMany = OneToMany.default
    ): Fn[I, I] = { row =>
      val traces = fn(row)

      val mayBeEmpty = traces.map { trace =>
        trace -> row.data
      }

      val result = forkType match {
        case OneToMany.Outer if mayBeEmpty.isEmpty => Seq(TraceSet.of(Trace.NoOp) -> row.data)
        case _                                     => mayBeEmpty
      }
      result
    }
  }
}

/**
  * Created by peng on 27/03/16
  *
  * TODO: the only difference between this and [[FlatMapPlan]] is the groupByKey step for sharing locality group, this
  * is too complex. the 2 classes should be merged
  */
case class FetchPlan[I, O: ClassTag](
    override val child: ExecutionPlan[I],
    fn: FetchPlan.Fn[I, O],
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

    val grouped: RDD[(LocalityGroup, Iterable[O])] = gpImpl.groupByKey(forkedRDD, beaconRDDOpt)

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
