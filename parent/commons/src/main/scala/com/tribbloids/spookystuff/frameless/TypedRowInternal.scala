package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.Hom
import com.tribbloids.spookystuff.frameless.TypedRow.ElementAPI
import shapeless.Poly2
import shapeless.ops.record.{Keys, MergeWith}

case class TypedRowInternal[L <: Tuple](
    cells: Vector[Any]
) {

  @transient lazy val repr: L = {
    cells
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

  protected trait ofData_Imp0 extends Hom.Poly {

    implicit def fromV[V]: V =>> TypedRow[Col_->>["value", V] *: Tuple.Empty] = at[V] { v =>
      ofTuple(Col_->>["value"](v) *: Tuple.empty)
    }
  }

  object ofData extends ofData_Imp0 {

    implicit def id[L <: Tuple]: TypedRow[L] =>> TypedRow[L] = at[TypedRow[L]] {
      identity[TypedRow[L]] _
    }
  }

  trait ElementWiseMethods extends Hom.Poly {

    import shapeless.record._

    val combineElements: Poly2

    type Theorem[L <: Tuple, R <: Tuple] = At[(TypedRow[L], TypedRow[R])]

    implicit def only[L <: Tuple, R <: Tuple](
        implicit
        lemma: MergeWith[L, R, combineElements.type]
    ): (TypedRow[L], TypedRow[R]) =>> TypedRow[lemma.Out] = at[(TypedRow[L], TypedRow[R])] { tuple =>
      val (left, right) = tuple
      val result = left._internal.repr.mergeWith(right._internal.repr)(combineElements)(lemma)
      TypedRowInternal.ofTuple(result)
    }

    case class MergeMethod[L <: Tuple](left: TypedRow[L]) {

      def apply[R <: Tuple](right: ElementAPI[R])(
          implicit
          ev: Theorem[L, R]
      ): ev.Out = {

        val _right = ElementAPI.unbox(right)
        ev.apply(left -> _right)
      }

    }

    case class CartesianProductMethod[L <: Tuple](left: Seq[TypedRow[L]]) {

      def apply[R <: Tuple](right: Seq[ElementAPI[R]])(
          implicit
          theorem: Theorem[L, R]
      ): Seq[theorem.Out] = {
        // cartesian product, size of output is the product of the sizes of 2 inputs

        for (
          ll <- left;
          method = MergeMethod(ll);
          rr <- right
        ) yield {

          method(rr)

//          MergeMethod(ll)(rr)
//          theorem(ll -> rr)
        }
      }

//      def apply[R <: Tuple](right: Seq[TypedRow.ElementView[R]])(
//          implicit
//          theorem: Theorem[L, R]
//      ): Seq[theorem.Out] = {
//        // cartesian product, size of output is the product of the sizes of 2 inputs
//
//        apply(right.map(_.asTypeRow))
//      }

      def apply[R <: Tuple](right: TypedRow.SeqView[R])(
          implicit
          theorem: Theorem[L, R]
      ): Seq[theorem.Out] = {
        // cartesian product, size of output is the product of the sizes of 2 inputs

        apply(right.asTypeRowSeq)
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
