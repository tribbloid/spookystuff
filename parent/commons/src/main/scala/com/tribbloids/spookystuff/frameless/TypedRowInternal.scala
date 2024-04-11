package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.Hom
import shapeless.Poly2
import shapeless.ops.record.{Keys, MergeWith}

import scala.collection.immutable.ArraySeq

case class TypedRowInternal[L <: Tuple](
    cells: ArraySeq[Any]
) {

  @transient lazy val repr: L = {
    cells
      .foldRight[Tuple](Tuple.Empty) { (s, x) =>
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

    new TypedRow[L](cells.to(ArraySeq))
  }

  protected trait ofData_Imp0 extends Hom.Poly {

    implicit def fromV[V]: V =>> TypedRow[Col_->>["value", V] *: Tuple.Empty] = at[V] { v =>
      ofTuple(Col_->>["value"](v) *: Tuple.Empty)
    }
  }

  object ofData extends ofData_Imp0 {

    implicit def id[L <: Tuple]: TypedRow[L] =>> TypedRow[L] = at[TypedRow[L]] {
      identity[TypedRow[L]] _
    }
  }

  trait Merge extends Hom.Poly {

    import shapeless.record._

    val fn: Poly2

    type Theorem[L <: Tuple, R <: Tuple] = At[(TypedRow[L], TypedRow[R])]

    implicit def only[L <: Tuple, R <: Tuple](
        implicit
        lemma: MergeWith[L, R, fn.type]
    ): (TypedRow[L], TypedRow[R]) =>> TypedRow[lemma.Out] = at[(TypedRow[L], TypedRow[R])] { tuple =>
      val (left, right) = tuple
      val result = left._internal.repr.mergeWith(right._internal.repr)(fn)(lemma)
      TypedRowInternal.ofTuple(result)
    }

    case class Curried[L <: Tuple](left: TypedRow[L]) {

      def apply[R <: Tuple](right: TypedRow[R])(
          implicit
          ev: Theorem[L, R]
      ): ev.Out = {

        ev.apply(left -> right)
      }
    }
  }

  object Merge {

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

    object keepRight extends Merge {

      object fn extends Poly2 {

        implicit def only[T, U]: Case.Aux[T, U, U] = at[T, U] { (_, r) =>
          r
        }
      }
    }

    object keepLeft extends Merge {

      object fn extends Poly2 {

        implicit def only[T, U]: Case.Aux[T, U, T] = at[T, U] { (l, _) =>
          l
        }
      }
    }

    object rigorous extends Merge {

      object fn extends Poly2 {
        // not allowed, compilation error
      }
    }
  }
}
