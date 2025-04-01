package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.execution.ExecutionPlan.CanChain
import com.tribbloids.spookystuff.row.*

object FlatMapPlan extends CatalystTypeOps.ImplicitMixin {

//  type Yield[O] = Data.Scoped[O]
  type Batch[O] = Seq[O]

  type Fn[I, O] = AgentRow[I] => Batch[O]

  object FlatMap {

    type _Fn[I, O] = AgentRow[I] => IterableOnce[O]

    def normalise[I, O](
        fn: _Fn[I, O]
    ): FlatMapPlan.this.Fn[I, O] = { row =>
      val result = fn(row)

      result.iterator.toSeq
    }
  }

  object Map {

    type _Fn[I, O] = AgentRow[I] => O

    def normalise[I, O](
        fn: _Fn[I, O]
    ): FlatMapPlan.this.Fn[I, O] = { row =>
      val result = fn(row)
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

  final override def prepare: SquashedRDD[O] = {

    val rdd = child.squashedRDD
    val result = rdd.map { squashed =>
      val rows: Seq[AgentRow[I]] = squashed.withCtx(child.ctx).unSquash

      val results = rows.flatMap { row =>
        fn(row).map { result =>
          result -> row.index
        }
      }

      squashed.copy(
        batch = results
      )
    }
    result
  }

  override lazy val normalisedPlan: ExecutionPlan[O] = {
    // if uncached, should be executed through others (FetchPlan & ExplorePlan)

    if (!this.isCached) {

      val _child = child.normalisedPlan

      _child match {
        case plan: CanChain[I] =>
          plan.chain(fn)
        case _ =>
          this
      }
    } else {
      this
    }

  }
}
