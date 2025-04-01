package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.commons.Types.Reduce
import com.tribbloids.spookystuff.row.{Data, LocalityGroup}

object Explore {

  type BatchK[+T] = Seq[(Data.Exploring[T], Int)]

  trait ReducerTypes[T] {

    type Exploring = Data.Exploring[T] // in open & visited cache, don't participate in ordering or reduce
    type Batch = BatchK[T]

    type RowOrdering = Ordering[(LocalityGroup, Batch)]

    type Reducer = Explore.ReducerK[T]
  }

  trait Common[I, O] {

    object Open extends ReducerTypes[I]

    object Visited extends ReducerTypes[O]

    type _Fn = ExplorePlan.Fn[I, O]

    // applied first, will be committed into visited
    // output should not tamper other fields of Data.Exploring[D]
//    type DeltaFn = SquashedRow[Elem] => SquashedRow[Out]

//    type ForkFn = AgentRow[Out] => Seq[Elem]
  }

  // TODO: how to simplify?
  trait ReducerK[T] extends ReducerTypes[T] with Reduce[BatchK[T]] with Serializable {

    def reduce(
        v1: this.Batch,
        v2: this.Batch
    ): this.Batch

    final override def apply(
        old: this.Batch,
        neo: this.Batch
    ): this.Batch = reduce(old, neo)
  }
}
