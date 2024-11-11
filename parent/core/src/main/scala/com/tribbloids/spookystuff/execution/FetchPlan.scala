package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.dsl.{GenPartitioner, OneToMany}
import com.tribbloids.spookystuff.execution.ExecutionPlan.CanChain
import com.tribbloids.spookystuff.row.*
import org.apache.spark.rdd.RDD

object FetchPlan {

  type Yield[O] = (HasTraceSet, Data.WithScope[O])
  // TODO: HasTraceSet will be gone, Agent can be manipulated directly
  // this will make it definitionally close to FlatMapPlan, the only difference is the shuffling

  type Batch[O] = Seq[Yield[O]]

  type Fn[I, O] = FetchedRow[I] => Batch[O]

  object TraceOnly {

    type _Fn[I] = FetchedRow[I] => HasTraceSet

    def normalise[I](
        fn: _Fn[I],
        forkType: OneToMany = OneToMany.default
    ): Fn[I, I] = { row =>
      val traces = fn(row)

      val mayBeEmpty = traces.asTraceSet.toSeq.map { trace =>
        trace -> row.payload
      }

      val result: Seq[(Trace, Data.WithScope[I])] = forkType match {
        case OneToMany.Outer if mayBeEmpty.isEmpty => Seq((Trace.NoOp: Trace) -> row.payload)
        case _                                     => mayBeEmpty
      }

      result
    }
  }
}

/**
  * Created by peng on 27/03/16
  *
  * TODO: the only difference between this and [[ChainPlan]] is the groupByKey step for sharing locality group, this is
  * too complex. the 2 classes should be merged
  */
case class FetchPlan[I, O](
    override val child: ExecutionPlan[I],
    fn: FetchPlan.Fn[I, O],
    sameBy: Trace => Any,
    genPartitioner: GenPartitioner
) extends UnaryPlan[I, O](child)
    with CanInjectBeaconRDD[O]
    with CanChain[O] {

  override def execute: SquashedRDD[O] = {

    val forkedRDD: RDD[(LocalityGroup, Data.WithScope[O])] = child.squashedRDD
      .flatMap { (v: SquashedRow[I]) =>
        val rows = v.withCtx(child.spooky).unSquash

        val seq = rows.flatMap(fn)

        seq.flatMap {
          case (ks, v) =>
            ks.asTraceSet.map { trace =>
              LocalityGroup(trace)().sameBy(sameBy) -> v
            }
        }
      }

    val grouped: RDD[(LocalityGroup, Iterable[Data.WithScope[O]])] = gpImpl.groupByKey(forkedRDD, beaconRDDOpt)

    grouped
      .map { tuple =>
        SquashedRow(
          tuple._1,
          tuple._2.toVector
        )

        // actual fetch can only be triggered by extract or savePages
      }
  }

  override def chain[O2](fn: ChainPlan.Fn[O, O2]): FetchPlan[I, O2] = {

    val newFn: FetchPlan.Fn[I, O2] = { row =>
      val out1 = this.fn(row)

      val out2 = out1.flatMap {
        case (trace, data) =>
          val row2 = FetchedRow(row.agentState, data)

          fn(row2).map { v =>
            trace -> v
          }
      }

      out2
    }

    this.copy(
      fn = newFn
    )
  }
}
