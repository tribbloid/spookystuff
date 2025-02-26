package com.tribbloids.spookystuff.linq.internal

import ai.acyclic.prover.commons.compat.TupleX
import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.linq.Linq.Row
import com.tribbloids.spookystuff.linq.Foundation.{KVBatchLike, KVPairs}
import com.tribbloids.spookystuff.linq.Foundation
import shapeless.Poly2
import shapeless.ops.record.MergeWith

trait ElementWisePoly extends Hom.Poly {

  import shapeless.record.*

  val combineElements: Poly2

  type LemmaAtRows[L <: TupleX, R <: TupleX] = Lemma.At[(Row[L], Row[R])]

  implicit def only[L <: TupleX, R <: TupleX](
      implicit
      lemma: MergeWith[L, R, combineElements.type]
  ): (Row[L], Row[R]) |- Row[lemma.Out] = at[(Row[L], Row[R])] { TupleX =>
    val (left, right) = TupleX
    val result = left._internal.repr.mergeWith(right._internal.repr)(combineElements)(lemma)
    RowInternal.ofTuple(result)
  }

  case class MergeMethod[L <: TupleX](left: Row[L]) {

    def apply[R <: TupleX](right: KVPairs[R])(
        implicit
        lemma: LemmaAtRows[L, R]
    ): lemma.Out = {

      val _right = KVPairs.unbox(right)
      val result: lemma.Out = lemma.apply(left -> _right)
      result
    }
  }

  /**
    * the following method can only be applied to 2 cases:
    *
    *   - single object that can be coerced into [[Record.SeqAPI]] (higher precedence)
    *   - Seq of [[Row.RowAPI]]
    *
    * Seq of objects that can be coerced into [[Row.RowAPI]] cannot be used as input directly.
    *
    * This is a deliberate design that prevents lists of [[Field.Named]] from participating in cartesian product
    * directly, consider use [[RowFunctions]].explode to explicitly convert it into Seq of [[Record]] instead
    */

  abstract class CartesianProductMethod_Lvl0[L <: TupleX](left: Seq[Row[L]]) {

    def apply[R <: TupleX](right: Seq[KVPairs[R]])(
        implicit
        lemma: LemmaAtRows[L, R]
    ): Seq[lemma.Out] = {
      // cartesian product, size of output is the product of the sizes of 2 inputs

      right.map { v =>
        v
      }

      val result: Seq[lemma.Out] =
        for (
          ll <- left;
          rr: KVPairs[R] <- right
        ) yield {
          val method: MergeMethod[L] = MergeMethod(ll)

          val result: lemma.Out = method(rr)
          result
        }

      result
    }
  }

  case class CartesianProductMethod[L <: TupleX](left: Seq[Row[L]]) extends CartesianProductMethod_Lvl0[L](left) {

    def apply[R <: TupleX](right: KVBatchLike[R])(
        implicit
        lemma: LemmaAtRows[L, R]
    ): Seq[lemma.Out] = {
      // cartesian product, size of output is the product of the sizes of 2 inputs

      val seq = Foundation.unbox(right)

      apply(seq)
    }
  }
}

object ElementWisePoly {

  // in Scala 3, all these objects can be both API and lemma
  // but it will take some time before Spark upgrade to it
  //    @deprecated
  //    object mayCauseDuplicates {
  //
  //      def apply[L2 <: TupleX](that: TypedRow[L2])(
  //          implicit
  //          ev: Merger[L, L2]
  //      ): TypedRow[ev.Out] = {
  //
  //        TypedRowInternal.ofTupleX(ev(_internal.repr, that._internal.repr))
  //      }
  //    }

  object preferRight extends ElementWisePoly {

    object combineElements extends Poly2 {

      implicit def only[T, U]: Case.Aux[T, U, U] = at[T, U] { (_, r) =>
        r
      }
    }
  }

  object preferLeft extends ElementWisePoly {

    object combineElements extends Poly2 {

      implicit def only[T, U]: Case.Aux[T, U, T] = at[T, U] { (l, _) =>
        l
      }
    }
  }

  object requireNoConflict extends ElementWisePoly {

    object combineElements extends Poly2 {
      // not allowed, compilation error
    }
  }
}
