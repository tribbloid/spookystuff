package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.frameless.Tuple.Empty
import shapeless.Poly2
import shapeless.ops.record.{Keys, MergeWith}

case class TypedRowInternal[L <: Tuple](
    runtimeVector: Vector[Any]
) {

  @transient def head[H](
      implicit
      ev: L <:< (H *: _)
  ): H = {

    runtimeVector.head.asInstanceOf[H]
  }

  @transient lazy val repr: L = {
    runtimeVector
      .foldRight[Tuple](Tuple.empty) { (s, x) =>
        s *: x
      }
      .asInstanceOf[L]
  }

  import shapeless.record._

  type Repr = L

  def keys(
      implicit
      ev: Keys[L]
  ): Keys[L]#Out = repr.keys
}

object TypedRowInternal {

  // TODO: remove, nameless columns is not supported in RecordEncoderField
  //  object ofArgs extends ProductArgs {
  //    def applyProduct[L <: Tuple](list: L): TypedRow[L] = fromTuple(list)
  //  }

  def ofTuple[L <: Tuple](
      record: L
  ): TypedRow[L] = {

    val cells = record.runtimeList

    new TypedRow[L](cells.to(Vector))
  }

  def ofElement[K <: XStr, V](
      v: K := V
  ): TypedRow[(K := V) *: Empty] = ofTuple(v *: Tuple.empty)

  protected trait ofData_Imp0 extends Hom.Poly {

    implicit def fromV[V]: V Target TypedRow[("value" := V) *: Tuple.Empty] = at[V] { v =>
      ofTuple((named["value"] := v) *: Tuple.empty)
    }
  }

  object ofData extends ofData_Imp0 {

    implicit def id[L <: Tuple]: TypedRow[L] Target TypedRow[L] = at[TypedRow[L]] {
      identity[TypedRow[L]] _
    }
  }

  trait ElementWiseMethods extends Hom.Poly {

    import shapeless.record._

    val combineElements: Poly2

    type LemmaAtRows[L <: Tuple, R <: Tuple] = LemmaAt[(TypedRow[L], TypedRow[R])]

    implicit def only[L <: Tuple, R <: Tuple](
        implicit
        lemma: MergeWith[L, R, combineElements.type]
    ): (TypedRow[L], TypedRow[R]) Target TypedRow[lemma.Out] = at[(TypedRow[L], TypedRow[R])] { tuple =>
      val (left, right) = tuple
      val result = left._internal.repr.mergeWith(right._internal.repr)(combineElements)(lemma)
      TypedRowInternal.ofTuple(result)
    }

    case class MergeMethod[L <: Tuple](left: TypedRow[L]) {

      def apply[R <: Tuple](right: TypedRow.ElementAPI[R])(
          implicit
          lemma: LemmaAtRows[L, R]
      ): lemma.Out = {

        val _right = TypedRow.ElementAPI.unbox(right)
        val result: lemma.Out = lemma.apply(left -> _right)
        result
      }
    }

    /**
      * the following method can only be applied to 2 cases:
      *
      *   - single object that can be coerced into [[TypedRow.SeqAPI]] (higher precedence)
      *   - Seq of [[TypedRow.ElementAPI]]
      *
      * Seq of objects that can be coerced into [[TypedRow.ElementAPI]] cannot be used as input directly.
      *
      * This is a deliberate design that prevents lists of [[Field.Named]] from participating in cartesian product
      * directly, consider use [[TypedRowFunctions]].explode to explicitly convert it into Seq of [[TypedRow]] instead
      */

    abstract class CartesianProductMethod_Lvl0[L <: Tuple](left: Seq[TypedRow[L]]) {

      def apply[R <: Tuple](right: Seq[TypedRow.ElementAPI[R]])(
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
            rr: TypedRow.ElementAPI[R] <- right
          ) yield {
            val method: MergeMethod[L] = MergeMethod(ll)

            val result: lemma.Out = method(rr)
            result
          }

        result
      }
    }

    case class CartesianProductMethod[L <: Tuple](left: Seq[TypedRow[L]]) extends CartesianProductMethod_Lvl0[L](left) {

      def apply[R <: Tuple](right: TypedRow.SeqAPI[R])(
          implicit
          lemma: LemmaAtRows[L, R]
      ): Seq[lemma.Out] = {
        // cartesian product, size of output is the product of the sizes of 2 inputs

        val seq = TypedRow.SeqAPI.unbox(right)

        apply(seq)
      }
    }
  }

  object ElementWiseMethods {

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

    object preferRight extends ElementWiseMethods {

      object combineElements extends Poly2 {

        implicit def only[T, U]: Case.Aux[T, U, U] = at[T, U] { (_, r) =>
          r
        }
      }
    }

    object preferLeft extends ElementWiseMethods {

      object combineElements extends Poly2 {

        implicit def only[T, U]: Case.Aux[T, U, T] = at[T, U] { (l, _) =>
          l
        }
      }
    }

    object requireNoConflict extends ElementWiseMethods {

      object combineElements extends Poly2 {
        // not allowed, compilation error
      }
    }
  }
}
