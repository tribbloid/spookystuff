package com.tribbloids.spookystuff.execution

import ai.acyclic.prover.commons.util.Magnet.PreferRightMagnet
import com.tribbloids.spookystuff.commons.refl.CatalystTypeOps
import com.tribbloids.spookystuff.execution.ExecutionPlan.CanChain
import com.tribbloids.spookystuff.row.*

import scala.reflect.ClassTag

object ChainPlan extends CatalystTypeOps.ImplicitMixin {

//  type Yield[O] = Data.Scoped[O]
  type Batch[O] = Seq[O]

  type Fn[I, O] = FetchedRow[I] => Batch[O]

  object FlatMap {

//    type ResultMag[O] = PreferRightMagnet[Seq[O], Batch[O]]
    type _Fn[I, O] = FetchedRow[I] => Batch[O]

    def normalise[I, O](
        fn: _Fn[I, O]
    ): ChainPlan.this.Fn[I, O] = { row =>
      val result = fn(row)

      result
    }
  }

  object Map {

//    type ResultMag[O] = PreferRightMagnet[O, Yield[O]]
    type _Fn[I, O] = FetchedRow[I] => O

    def normalise[I, O](fn: _Fn[I, O]): ChainPlan.this.Fn[I, O] = { row =>
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

case class ChainPlan[I, O: ClassTag]( // narrow means narrow transformation in Apache Spark
    override val child: ExecutionPlan[I],
    fn: ChainPlan.Fn[I, O]
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

  override def normalise: ExecutionPlan[O] = {
    // if uncached, should be executed through others (FetchPlan & ExplorePlan)

    if (!this.isCached) {

      val _child = child.normalise

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
