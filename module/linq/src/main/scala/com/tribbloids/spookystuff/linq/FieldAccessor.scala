package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.compat.NamedTupleX.:=
import ai.acyclic.prover.commons.compat.TupleX.{*:, T1}
import ai.acyclic.prover.commons.compat.{Key, TupleX, XStr}
import com.tribbloids.spookystuff.linq.Foundation.CellLike
import com.tribbloids.spookystuff.linq.internal.ElementWisePoly
import shapeless.ops.record.{Remover, Selector}
import scala.language.dynamics

case class FieldAccessor[T <: TupleX](
    rec: Rec[T]
) extends Dynamic {

  type FieldSelectorAux[K, V] = Selector.Aux[T, Key.Tag[K], V]

  sealed class FieldSelection[
      K <: XStr, // index, CAUTION: this is neither a key nor a string, in shapeless record it is usually a Symbol defined by @@
      V
  ]()(
      implicit
      val selector: FieldSelectorAux[K, V]
      //        val remover: FieldRemover[K]
  ) extends CellLike[T1[K := V]] {

    type FieldRemover = Remover[T, Key.Tag[K]]
    type FieldRemoverAux[O2 <: TupleX] = Remover.Aux[T, Key.Tag[K], (V, O2)]

    lazy val value_tagged: K := V = value.asInstanceOf[K := V]

    lazy val value: V = {
      selector(rec._internal.repr)
    }

    lazy val asRow: Rec[(K := V) *: TupleX.T0] = {
      Rec.ofTupleX((Key[K] := value_tagged) *: TupleX.T0)
    }

    object remove {

      def apply[O2 <: TupleX]()(
          implicit
          exactRemover: FieldRemoverAux[O2]
      ): Rec[O2] = {

        val tuple = exactRemover.apply(rec._internal.repr)
        val after: O2 = tuple._2

        Rec.ofTupleX(after)
      }

      def asTuple()(
          implicit
          remover: FieldRemover
      ) = {

        val tuple = remover.apply(rec._internal.repr)
        tuple

      }
    }
    def drop = remove

    object set { // TODO: name usually associated with in-place update, should use copy() or update() instead

      def apply[VV](value: VV)(
          implicit
          ev: ElementWisePoly.preferRight.LemmaAtRows[T, (K := VV) *: TupleX.T0]
      ): ev.Out = {

        val neo: Rec[(K := VV) *: TupleX.T0] = Rec.ofTupleX((Key[K] := value) *: TupleX.T0)

        val result = ev.apply(rec -> neo)

        result
      }
    }
    def := : set.type = set

    /**
      * To be used in [[org.apache.spark.sql.Dataset]].flatMap
      */

    //    def explode[R](
    //        fn: V => Seq[R]
    //    )(
    //        implicit
    //        ev1: Merge.keepRight.Theorem[L, (K ->> R) *: Tuple.Empty]
    //    ): Seq[ev1.Out] = {
    //
    //      val results = valueWithField.map { v: V =>
    //        val r = fn(v)
    //        set(r)(ev1)
    //      }
    //      results
    //    }

  }

  def selectDynamic(key: XStr)(
      implicit
      selector: Selector[T, Key.Tag[key.type]]
      //          remover: Remover[T, key.type]
  ) = new FieldSelection[key.type, selector.Out]()(selector)
}

object FieldAccessor {}
