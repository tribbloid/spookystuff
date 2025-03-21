package com.tribbloids.spookystuff.linq

import ai.acyclic.prover.commons.compat.NamedTupleX.:=
import ai.acyclic.prover.commons.compat.TupleX.{*:, T0, T1}
import ai.acyclic.prover.commons.compat.{Key, TupleX, XStr}
import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.linq
import com.tribbloids.spookystuff.linq.Foundation.{CellLike, KVBatch, RecordLike}
import com.tribbloids.spookystuff.linq.internal.{ElementWisePoly, RecordInternal}
import shapeless.RecordArgs

import scala.language.dynamics

object Record extends RecordArgs {

  def applyRecord[L <: TupleX](list: L): Record[L] = Record.ofTuple(list)

  def ofTuple[L <: TupleX](
      data: L
  ): linq.Record[L] = {

    val cells = data.runtimeList

    new linq.Record[L](cells.to(Vector))
  }

  def ofTagged[K <: XStr, V](
      v: K := V
  ): linq.Record[(K := V) *: T0] = ofTuple(v *: TupleX.T0)

  sealed protected trait OfData_Imp0 extends Hom.Poly {

    implicit def fromValue[V]: V |- linq.Record[("value" := V) *: TupleX.T0] = at[V] { v =>
      ofTuple((Key["value"] := v) *: TupleX.T0)
    }
  }

  object ofData extends OfData_Imp0 {
    // can be used to convert value of any type into a Record

    implicit def id[L <: TupleX]: linq.Record[L] |- linq.Record[L] = at[linq.Record[L]] {
      identity[linq.Record[L]]
    }
  }

  implicit class _rowSeqView[T <: TupleX](
      val rows: Seq[Record[T]]
  ) extends Foundation.LeftOpsMixin[T]
      with KVBatch[T] {

    // cartesian product can be directly called on Seq
  }
}

/**
  * shapeless Tuple & Record has high runtime overhead and poor Scala 3 compatibility. its usage should be minimized
  *
  * do not use shapeless instances for data storage/shipping
  *
  * @param runtimeData
  *   data
  * @tparam T
  *   Record type
  */
final class Record[T <: TupleX](
    runtimeData: Vector[Any] // TODO: should use unboxed binary data structure, Java 21 or Apache Arrow maybe helpful
    // TODO: there should be a few 3rd party libraries in Scala 3
) extends Dynamic
    with RecordLike[T] {

  // TODO: how to easily reconstruct vertices/edges for graphX/graphframe?
  //  since graphframe table always demand id/src/tgt columns, should the default
  //  representation be SemiRow? that contains both structured and newType part?

  import shapeless.ops.record.*

  /**
    * Allows dynamic-style access to fields of the record whose keys are Symbols. See
    * [[shapeless.syntax.DynamicRecordOps[_]] for original version
    *
    * CAUTION: this takes all the slots for nullary fields, none the following functions will be nullary
    */
  def selectDynamic(key: XStr)(
      implicit
      selector: Selector[T, Key.Tag[key.type]]
      //        remover: Remover[T, key.type]
  ): key.type := selector.Out = {

    val value: selector.Out = _fields.selectDynamic(key).value

    Key[key.type] := value

    //      Field.Named[key.type].apply(value: selector.Out)

  }

  @transient override lazy val toString: String = runtimeData.mkString("[", ",", "]")

  type FieldSelectorAux[K, V] = Selector.Aux[T, Key.Tag[K], V]

  sealed class FieldSelection[
      K <: XStr, // index, CAUTION: this is neither a key nor a string, in shapeless record it is usually a Symbol defined by @@
      V
  ]()(
      implicit
      val selector: FieldSelectorAux[K, V]
      //        val remover: FieldRemover[K]
  ) extends CellLike[T1[K := V]] { // TODO: merge with RecordEntryAsCell

    type FieldRemover = Remover[T, Key.Tag[K]]
    type FieldRemoverAux[O2 <: TupleX] = Remover.Aux[T, Key.Tag[K], (V, O2)]

    lazy val value_tagged: K := V = value.asInstanceOf[K := V]

    lazy val value: V = {
      selector(_internal.repr)
    }

    lazy val asRow: Record[(K := V) *: TupleX.T0] = {
      Record.ofTuple((Key[K] := value_tagged) *: TupleX.T0)
    }

    object remove {

      def apply[O2 <: TupleX]()(
          implicit
          exactRemover: FieldRemoverAux[O2]
      ): Record[O2] = {

        val tuple = exactRemover.apply(_internal.repr)
        val after: O2 = tuple._2

        Record.ofTuple(after)
      }

      def asTuple()(
          implicit
          remover: FieldRemover
      ) = {

        val tuple = remover.apply(_internal.repr)
        tuple

      }
    }
    def drop = remove

    object set { // TODO: name usually associated with in-place update, should use copy() or update() instead

      def apply[VV](value: VV)(
          implicit
          ev: ElementWisePoly.preferRight.LemmaAtRows[T, (K := VV) *: TupleX.T0]
      ): ev.Out = {

        val neo: Record[(K := VV) *: TupleX.T0] = Record.ofTuple((Key[K] := value) *: TupleX.T0)

        val result = ev.apply(Record.this -> neo)

        result.asInstanceOf[ev.Out]
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

  object _fields extends Dynamic {

    def selectDynamic(key: XStr)(
        implicit
        selector: Selector[T, Key.Tag[key.type]]
        //          remover: Remover[T, key.type]
    ) = new FieldSelection[key.type, selector.Out]()(selector)
  }

  @transient lazy val _internal: RecordInternal[T] = {

    RecordInternal(runtimeData)
  }

}
