package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.util.Magnet.PreferRightMagnet
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.dsl.{GenPartitioner, Sampler}
import com.tribbloids.spookystuff.execution.ExecutionPlan.CanChain
import com.tribbloids.spookystuff.row.*
import org.apache.spark.rdd.RDD

object FetchPlan {

  type Yield[O] = (Trace, Data.Scoped[O])
  // TODO: HasTraceSet will be gone, Agent can be manipulated directly
  // this will make it definitionally close to FlatMapPlan, the only difference is the shuffling

  type Batch[O] = Seq[Yield[O]]

  type Fn[I, O] = FetchedRow[I] => Batch[O]

  object Invar {

    type ResultMag[I] = PreferRightMagnet[HasTraceSet, (HasTraceSet, I)]
    type _Fn[I] = FetchedRow[I] => ResultMag[I]

    def normalise[I](
        fn: _Fn[I],
        sampler: Sampler = Sampler.Identity
    ): Fn[I, I] = { row =>
      val mag = fn(row)

      val normalised: (HasTraceSet, Data.Scoped[I]) = mag.revoke match {
        case Left(traces) =>
          traces -> row.payload
        case Right(v) =>
          v._1 -> row.payload.copy(data = v._2)
      }

      val flat: Seq[Yield[I]] = normalised._1.asTraceSet.map { trace =>
        trace -> normalised._2
      }.toSeq

      val sampled = sampler.apply[Yield[I]](flat).map { (opt: Option[Yield[I]]) =>
        opt.getOrElse {
          val default: Yield[I] = Trace.NoOp.trace -> normalised._2
          default
        }
      }

      sampled
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

    val forkedRDD: RDD[(LocalityGroup, Data.Scoped[O])] = child.squashedRDD
      .flatMap { (v: SquashedRow[I]) =>
        val rows = v.withCtx(child.spooky).unSquash

        val seq = rows.flatMap(fn)

        seq.map {
          case (trace, v) =>
            LocalityGroup(trace)().sameBy(sameBy) -> v
        }
      }

    val grouped: RDD[(LocalityGroup, Iterable[Data.Scoped[O]])] = gpImpl.groupByKey(forkedRDD, beaconRDDOpt)

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

      val out2: Seq[(Trace, Data.Scoped[O2])] = out1.flatMap {
        case (traces, data) =>
          val row2 = FetchedRow(row.agentState, data)

          fn(row2).map { v =>
            traces -> v
          }
      }

      out2
    }

    this.copy(
      fn = newFn
    )
  }
}
