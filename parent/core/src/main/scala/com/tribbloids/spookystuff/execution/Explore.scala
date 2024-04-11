package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.TraceSet
import com.tribbloids.spookystuff.commons.Types.Reduce
import com.tribbloids.spookystuff.row.{Data, FetchedRow, LocalityGroup}

object Explore {

  /**
    * [[ChainPlan.Out]] deliberately contains [[Data.WithScope]], but the scope will not be commited into the visited
    * set. it is only there to make appending [[ChainPlan]] easier
 *
    * @tparam I
    *   input
    * @tparam O
    *   output
    */
  type Out[I, O] = (ForkPlan.Out[I], ChainPlan.Out[O])

  type Fn[I, O] = FetchedRow[Data.Exploring[I]] => Out[I, O]

  trait ReducerLike[I] {

    type Elem = Data.Exploring[I]
    type Elems = Vector[Elem]

    type RowOrdering = Ordering[(LocalityGroup, Vector[Elem])]

    type FetchFn = FetchedRow[I] => TraceSet
  }

  trait Common[I, O] extends ReducerLike[I] {

    type Out = Data.Exploring[O]
    type Outs = Vector[Out]

    type OpenReducer = Explore.ReducerK[I]
    type VisitedReducer = Explore.ReducerK[O]

    type Fn = Explore.Fn[I, O]

    // applied first, will be committed into visited
    // output should not tamper other fields of Data.Exploring[D]
//    type DeltaFn = SquashedRow[Elem] => SquashedRow[Out]

//    type ForkFn = FetchedRow[Out] => Seq[Elem]
  }

  // TODO: how to simplify?
  trait ReducerK[D] extends ReducerLike[D] with Reduce[Vector[Data.Exploring[D]]] with Serializable {

    def reduce(
        v1: Elems,
        v2: Elems
    ): Elems

    final override def apply(
        old: Elems,
        neo: Elems
    ): Elems = reduce(old, neo)
  }
}
