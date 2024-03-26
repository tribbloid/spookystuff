package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.Hom
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}
import shapeless.{Poly2, RecordArgs}

import scala.collection.immutable.ArraySeq
import scala.language.dynamics
import scala.reflect.ClassTag

/**
  * shapeless Tuple & Record has high runtime overhead and poor Scala 3 compatibility. its usage should be minimized
  *
  * do not use shapeless instances for data storage/shipping
  *
  * @param cells
  *   data
  * @tparam L
  *   Record type
  */
class TypedRow[L <: Tuple](
    cells: ArraySeq[Any]
) {
  // TODO: how to easily reconstruct vertices/edges for graphX/graphframe?
  //  since graphframe table always demand id/src/tgt columns, should the default
  //  representation be SemiRow? that contains both structured and newType part?

  import shapeless.ops.record._
  import shapeless.record._

  @transient override lazy val toString: String = cells.mkString("[", ",", "]")

  // DO NOT RENAME! used by reflection-based Catalyst Encoder
  def _valueAtIndex(i: Int): Any = cells.apply(i)

  sealed class FieldView[K, V](
      val key: K
  )(
      val selector: Selector.Aux[L, K, V]
  ) {

    lazy val valueWithField: V = selector(_internal.repr)

    lazy val value: V = {
      valueWithField // TODO: should remove Field capability mixins
    }

    lazy val asTypedRow: TypedRow[(K ->> V) *: Tuple.Empty] = {
      TypedRow.ofTuple(->>[K](valueWithField) *: Tuple.Empty)
    }

    //    lazy val value: V = selector(asRepr)

    //    object remove {
    //
    //      def apply[L2 <: Tuple, O2 <: Tuple]()(
    //          implicit
    //          ev: Remover.Aux[L, K, (Any, O2)]
    //      ): TypedRow[O2] = {
    //
    ////        val tuple = repr.remove(key)(ev)
    //        val tuple = ev.apply(repr)
    //
    //        TypedRow.ofTuple(tuple._2)
    //      }
    //    }
    //    def - : remove.type = remove

    object set {

      def apply[VV](value: VV)(
          implicit
          ev0: MergeWith[L, (K ->> VV) *: Tuple.Empty, _merge.keepRight.fn.type]
      ): TypedRow[ev0.Out] = {

        val neo: TypedRow[(K ->> VV) *: Tuple.Empty] = TypedRow.ofTuple(->>[K](value) *: Tuple.Empty)
        val result = _merge.keepRight(neo)(ev0)
        result
      }
    }
    def := : set.type = set

    /**
      * To be used in [[org.apache.spark.sql.Dataset]].flatMap
      */

    def explode[VV, R](
        fn: VV => R
    )(
        implicit
        ev0: V <:< Seq[VV],
        ev1: MergeWith[L, (K ->> R) *: Tuple.Empty, _merge.keepRight.fn.type]
    ): Seq[TypedRow[ev1.Out]] = {

      val results = valueWithField.map { v: VV =>
        val r = fn(v)
        set(r)(ev1)
      }
      results
    }
  }

  object _fields extends Dynamic {

    def selectDynamic(key: String with Singleton)(
        implicit
        selector: Selector[L, Col[key.type]]
    ) = new FieldView[Col[key.type], selector.Out](Col(key))(selector)
  }

  @transient lazy val values: TypedRow.ProductView[L] = new TypedRow.ProductView(this)

  @transient lazy val _internal: TypedRowInternal[L] = {

    val repr = cells
      .foldRight[Tuple](Tuple.Empty) { (s, x) =>
        s *: x
      }
      .asInstanceOf[L]

    TypedRowInternal(repr)
  }

  object update {}

  object _merge {

    trait MergeWithFn {

      val fn: Poly2

      def apply[L2 <: Tuple](that: TypedRow[L2])(
          implicit
          ev0: MergeWith[L, L2, fn.type]
      ): TypedRow[ev0.Out] = {

        val result = _internal.repr.mergeWith(that._internal.repr)(fn)(ev0)
        TypedRow.ofTuple(result)
      }
    }

    // in Scala 3, all these objects can be both API and lemma
    // but it will take some time before Spark upgrade to it
    @deprecated
    object mayCauseDuplicates {

      def apply[L2 <: Tuple](that: TypedRow[L2])(
          implicit
          ev: Merger[L, L2]
      ): TypedRow[ev.Out] = {

        TypedRow.ofTuple(ev(_internal.repr, that._internal.repr))
      }
    }

    object keepRight extends MergeWithFn {

      object fn extends Poly2 {

        implicit def only[T, U]: Case.Aux[T, U, U] = at[T, U] { (_, r) =>
          r
        }
      }
    }

    object keepLeft extends MergeWithFn {

      object fn extends Poly2 {

        implicit def only[T, U]: Case.Aux[T, U, T] = at[T, U] { (l, _) =>
          l
        }
      }
    }
  }

  @deprecated
  def ++ : _merge.mayCauseDuplicates.type = _merge.mayCauseDuplicates

  def ++< : _merge.keepRight.type = _merge.keepRight

  def >++ : _merge.keepLeft.type = _merge.keepLeft
}

object TypedRow extends TypedRowOrdering.Default.Giver {

  import shapeless.ops.record._

  // TODO: this is the actual TypedRow, the main class is the internal
  class ProductView[T <: Tuple](internal: TypedRow[T]) extends Dynamic {

    private def fields: internal._fields.type = internal._fields

    /**
      * Allows dynamic-style access to fields of the record whose keys are Symbols. See
      * [[shapeless.syntax.DynamicRecordOps[_]] for original version
      *
      * CAUTION: this takes all the slots for nullary fields, none the following functions will be nullary
      */
    def selectDynamic(key: String with Singleton)(
        implicit
        selector: Selector[T, Col[key.type]]
    ): selector.Out = {

      fields.selectDynamic(key).value
    }
  }

  object ProductView extends RecordArgs {

    def applyRecord[L <: Tuple](list: L): TypedRow[L] = ofTuple(list)
  }

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

    implicit def passThrough[L <: Tuple]: TypedRow[L] =>> TypedRow[L] = at[TypedRow[L]] {
      identity[TypedRow[L]] _
    }
  }

  case class WithCatalystTypes(schema: Seq[DataType]) {

    // DO NOT RENAME! used by reflection-based Catalyst Encoder
    def fromInternalRow(row: InternalRow): TypedRow[Tuple] = {
      val data = row.toSeq(schema)

      val seq = data.to(ArraySeq)
      new TypedRow[Tuple](seq)
    }
  }

  //  object WithCatalystTypes {}

  lazy val catalystType: ObjectType = ObjectType(classOf[TypedRow[_]])

  implicit def getEncoder[G <: Tuple](
      implicit
      stage1: RecordEncoderStage1[G, G],
      classTag: ClassTag[TypedRow[G]]
  ): TypedEncoder[TypedRow[G]] = RecordEncoder.ForTypedRow[G, G]()

}
