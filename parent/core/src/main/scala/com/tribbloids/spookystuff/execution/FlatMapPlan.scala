package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.util.Magnet.PreferRightMagnet
import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.row._

object FlatMapPlan extends CatalystTypeOps.ImplicitMixin {

  type Out[O] = Data.WithScope[O]
  type Batch[O] = Seq[Data.WithScope[O]]

  type Fn[I, O] = FetchedRow[I] => Batch[O]

  object FlatMap {

    type RMag[O] = PreferRightMagnet[Seq[O], Batch[O]]
    type _Fn[I, O] = FetchedRow[I] => RMag[O]

    def normalise[I, O](fn: _Fn[I, O]): FlatMapPlan.this.Fn[I, O] = { row =>
      val mag = fn(row).revoke

      val result: Batch[O] = mag match {
        case Left(vs: Seq[O]) =>
          vs.map { v =>
            row.dataWithScope.copy(data = v)
          }
        case Right(vs: Batch[O]) =>
          vs
      }
      result
    }
  }

  object Map {

    type RMag[O] = PreferRightMagnet[O, Out[O]]
    type _Fn[I, O] = FetchedRow[I] => PreferRightMagnet[O, Out[O]]

    def normalise[I, O](fn: _Fn[I, O]): FlatMapPlan.this.Fn[I, O] = { row =>
      val result = fn(row).revoke match {
        case Left(v) =>
          row.dataWithScope.copy(data = v)
        case Right(v) =>
          v
      }
      Seq(result)

    }
  }

//  case class ExplodeScope[I](
//      scopeFn: Data.WithScope[I] => Seq[Data.WithScope[I]]
//  ) extends Fn[I, I] {
//
//    override def apply(src SquashedRow[I]) :=> SquashedRow[I] = :=> { v =>
//      v.flatMapData(scopeFn)
//    }
//  }

}

case class FlatMapPlan[I, O]( // narrow means narrow transformation in Apache Spark
    override val child: ExecutionPlan[I],
    fn: FlatMapPlan.Fn[I, O]
) extends UnaryPlan[I, O](child) {

  final override def execute: SquashedRDD[O] = {

    val rdd = child.squashedRDD
    val result = rdd.map { squashed =>
      val rows: Seq[FetchedRow[I]] = squashed.withSchema(child.outputSchema).unSquash

      val results = rows.flatMap { row =>
        fn(row)
      }

      squashed.copy(
        batch = results
      )
    }
    result
  }

  def optimse(
  ): UnaryPlan[I, O] = {

    this
    //    this match { // TODO: enable this optimisation later
    //      case plan: ExplorePlan[_, _] if !this.isCached =>
    //        object _More extends Explore.Fn[D, O] {
    //
    //          override def apply(row: FetchedRow[Data.Exploring[D]]) = {
    //
    //            val (forked, flat) = plan.fn(row)
    //
    //            flat.map(withScope => ???)
    //            ???
    //          }
    //        }
    //
    //        plan.copy()(_More)
    //      case _ =>
    //        FlatPlan(this, fn)
    //    }
  }
}
