package com.tribbloids.spookystuff.linq.internal

import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.linq.Linq.Row
import com.tribbloids.spookystuff.linq.LinqBase.{Batch, Entry}
import com.tribbloids.spookystuff.linq.{LinqBase, Tuple}
import shapeless.Poly2
import shapeless.ops.record.MergeWith

trait ElementWisePoly extends Hom.Poly {

  import shapeless.record._

  val combineElements: Poly2

  type LemmaAtRows[L <: Tuple, R <: Tuple] = LemmaAt[(Row[L], Row[R])]

  implicit def only[L <: Tuple, R <: Tuple](
      implicit
      lemma: MergeWith[L, R, combineElements.type]
  ): (Row[L], Row[R]) Target Row[lemma.Out] = at[(Row[L], Row[R])] { tuple =>
    val (left, right) = tuple
    val result = left._internal.repr.mergeWith(right._internal.repr)(combineElements)(lemma)
    RowInternal.ofTuple(result)
  }

  case class MergeMethod[L <: Tuple](left: Row[L]) {

    def apply[R <: Tuple](right: Entry[R])(
        implicit
        lemma: LemmaAtRows[L, R]
    ): lemma.Out = {

      val _right = Entry.unbox(right)
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

  abstract class CartesianProductMethod_Lvl0[L <: Tuple](left: Seq[Row[L]]) {

    def apply[R <: Tuple](right: Seq[Entry[R]])(
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
          rr: Entry[R] <- right
        ) yield {
          val method: MergeMethod[L] = MergeMethod(ll)

          val result: lemma.Out = method(rr)
          result
        }

      result
    }
  }

  case class CartesianProductMethod[L <: Tuple](left: Seq[Row[L]]) extends CartesianProductMethod_Lvl0[L](left) {

    def apply[R <: Tuple](right: Batch[R])(
        implicit
        lemma: LemmaAtRows[L, R]
    ): Seq[lemma.Out] = {
      // cartesian product, size of output is the product of the sizes of 2 inputs

      val seq = LinqBase.unbox(right)

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
  //      def apply[L2 <: Tuple](that: TypedRow[L2])(
  //          implicit
  //          ev: Merger[L, L2]
  //      ): TypedRow[ev.Out] = {
  //
  //        TypedRowInternal.ofTuple(ev(_internal.repr, that._internal.repr))
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
