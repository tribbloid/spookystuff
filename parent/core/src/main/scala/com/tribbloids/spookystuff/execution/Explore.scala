package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.commons.Types.Reduce
import com.tribbloids.spookystuff.row.{Data, LocalityGroup}

object Explore {

  trait ReducerLike[I] {

    type Exploring = Data.Exploring[I]
    type Batch = Vector[Exploring]

    type RowOrdering = Ordering[(LocalityGroup, Vector[Exploring])]

//    type FetchFn = FetchedRow[I] => TraceSet
  }

  trait Common[I, O] extends ReducerLike[I] {

    type _Exploring = Data.Exploring[O]
    type _Batch = Vector[_Exploring]

    type OpenReducer = Explore.ReducerK[I]
    type VisitedReducer = Explore.ReducerK[O]

    type _Fn = ExplorePlan.Fn[I, O]

    // applied first, will be committed into visited
    // output should not tamper other fields of Data.Exploring[D]
//    type DeltaFn = SquashedRow[Elem] => SquashedRow[Out]

//    type ForkFn = FetchedRow[Out] => Seq[Elem]
  }

  // TODO: how to simplify?
  trait ReducerK[D] extends ReducerLike[D] with Reduce[Vector[Data.Exploring[D]]] with Serializable {

    def reduce(
        v1: Batch,
        v2: Batch
    ): Batch

    final override def apply(
        old: Batch,
        neo: Batch
    ): Batch = reduce(old, neo)
  }
}
