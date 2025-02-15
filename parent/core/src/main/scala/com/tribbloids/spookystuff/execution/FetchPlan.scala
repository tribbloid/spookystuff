package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.execution.ExecutionPlan.CanChain
import com.tribbloids.spookystuff.row.*
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object FetchPlan {
  // TODO: HasTraceSet will be gone, Agent can be manipulated directly
  // this will make it definitionally close to FlatMapPlan, the only difference is the shuffling

  type Batch[O] = Seq[(Trace, O)]

  type Fn[I, O] = FetchedRow[I] => Batch[O]

//  object ToTraceSet {
//
//    type Result[O] = (HasTraceSet, O)
//    // here, we can to cast Fetch into (Fetch, I), despite Fetch <:< K[Nothing] and can be cast into any K[O]
//
//    type _Fn[I, O] = FetchedRow[I] => (HasTraceSet, O)
//
//    // here, we need to ensure to
//    def normalise[I, O](
//        fn: _Fn[I, O],
//        sampler: Sampler = Sampler.Identity
//    ): Fn[I, O] = { row =>
//      val normalised = fn(row)
//
//      val flat: Batch[O] = normalised._1.traceSet.map { trace =>
//        trace -> normalised._2
//      }.toSeq
//
//      val sampled = sampler.apply(flat).map { opt =>
//        opt.getOrElse {
//          val default = Trace.NoOp.asTrace -> normalised._2
//          default
//        }
//      }
//
//      sampled
//    }
//  }
//
//  object Invar {
//
//    type Result[I] = PreferRightMagnet[HasTraceSet, (HasTraceSet, I)]
//
//    type _Fn[I] = FetchedRow[I] => Result[I]
//
//    def normalise[I](
//        fn: _Fn[I],
//        sampler: Sampler = Sampler.Identity
//    ): Fn[I, I] = {
//
//      val result = ToTraceSet.normalise[I, I](
//        { row =>
//          val mag = fn(row)
//
//          val normalised: (HasTraceSet, I) = mag.original match {
//            case Left(traces) =>
//              traces -> row.data
//            case Right(v) =>
//              v._1 -> v._2
//          }
//
//          normalised
//        },
//        sampler
//      )
//
//      result
//    }
//  }
}

/**
  * Created by peng on 27/03/16
  *
  * TODO: the only difference between this and [[ChainPlan]] is the groupByKey step for sharing locality group, this is
  * too complex. the 2 classes should be merged
  */
case class FetchPlan[I, O: ClassTag](
    override val child: ExecutionPlan[I],
    fn: FetchPlan.Fn[I, O],
    sameBy: Trace => Any,
    genPartitioner: GenPartitioner
) extends UnaryPlan[I, O](child)
    with CanInjectBeaconRDD[O]
    with CanChain[O] {

  override def prepare: SquashedRDD[O] = {

    val sketched: RDD[(LocalityGroup, O)] = child.squashedRDD
      .flatMap { (v: SquashedRow[I]) =>
        val rows = v.withCtx(child.spooky).unSquash

        val seq = rows.flatMap(fn)

        seq.map {
          case (trace, v) =>
            LocalityGroup(trace)().sameBy(sameBy) -> v
        }
      }

    val grouped: RDD[(LocalityGroup, Iterable[O])] = gpImpl.groupByKey(sketched, beaconRDDOpt)

    grouped
      .map { tuple =>
        SquashedRow(
          tuple._1,
          tuple._2.toVector
        )

        // actual fetch can only be triggered by extract or savePages
      }
  }

  override def chain[O2: ClassTag](fn: ChainPlan.Fn[O, O2]): FetchPlan[I, O2] = {

    val newFn: FetchPlan.Fn[I, O2] = { row =>
      val out1 = this.fn(row)

      val out2: Seq[(Trace, O2)] = out1.flatMap {
        case (traces, data) =>
          val row2 = row.copy(data = data)

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
