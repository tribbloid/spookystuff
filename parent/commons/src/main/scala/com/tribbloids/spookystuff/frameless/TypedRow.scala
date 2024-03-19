package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.Hom
import com.tribbloids.spookystuff.frameless.Field.CanSort
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
case class TypedRow[L <: Tuple](
    cells: ArraySeq[Any]
) {
  // TODO: how to easily reconstruct vertices/edges for graphX/graphframe?
  //  since graphframe table always demand id/src/tgt columns, should the default
  //  representation be SemiRow? that contains both structured and newType part?

  import shapeless.record._
  import shapeless.ops.record._

  type Repr = L

  @transient override lazy val toString: String = cells.mkString("[", ",", "]")

  // DO NOT RENAME! used by reflection-based Catalyst Encoder
  def _valueAtIndex(i: Int): Any = cells.apply(i)

  sealed class FieldView[K, V](
      val key: K
  )(
      val selector: Selector.Aux[L, K, V]
  ) {

    lazy val valueWithField: V = selector(repr)

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

    object update {

      def apply[VV](value: VV)(
          implicit
          ev0: MergeWith[L, (K ->> VV) *: Tuple.Empty, mergeKeepRight.fn.type]
      ): TypedRow[ev0.Out] = {

        val neo: TypedRow[(K ->> VV) *: Tuple.Empty] = TypedRow.ofTuple(->>[K](value) *: Tuple.Empty)
        val result = mergeKeepRight(neo)(ev0)
        result
      }
    }
    def + : update.type = update

    /**
      * To be used in [[org.apache.spark.sql.Dataset]].flatMap
      */

    def explode[VV, R](
        fn: VV => R
    )(
        implicit
        ev0: V <:< Seq[VV],
        ev1: MergeWith[L, (K ->> R) *: Tuple.Empty, mergeKeepRight.fn.type]
    ): Seq[TypedRow[ev1.Out]] = {

      val results = valueWithField.map { v: VV =>
        val r = fn(v)
        update(r)(ev1)
      }
      results
    }
  }

  object fields extends Dynamic {

    def selectDynamic(key: String with Singleton)(
        implicit
        selector: Selector[L, Col[key.type]]
    ) = new FieldView[Col[key.type], selector.Out](Col(key))(selector)
  }

  @transient lazy val values: TypedRow.DynamicProductAPI[L] = TypedRow.DynamicProductAPI(this)

  @transient lazy val repr: L = cells
    .foldRight[Tuple](Tuple.Empty) { (s, x) =>
      s *: x
    }
    .asInstanceOf[L]

  def canSortAll(
      implicit
      ev: MapValues[CanSort.Enable.asShapeless.type, L]
  ): TypedRow[ev.Out] = {

    val mapped = repr.mapValues(CanSort.Enable)(ev)

    TypedRow.ofTuple(mapped)
  }

  def keys(
      implicit
      ev: Keys[L]
  ): Keys[L]#Out = repr.keys

  // in Scala 3, all these objects can be both API and lemma
  // but it will take some time before Spark upgrade to it
  @deprecated
  object merge_mayCauseDuplicates {

    def apply[L2 <: Tuple](that: TypedRow[L2])(
        implicit
        ev: Merger[L, L2]
    ): TypedRow[ev.Out] = {

      TypedRow.ofTuple(ev(repr, that.repr))
    }
  }
  @deprecated
  def ++ : merge_mayCauseDuplicates.type = merge_mayCauseDuplicates

  trait MergeWithFn {

    val fn: Poly2

    def apply[L2 <: Tuple](that: TypedRow[L2])(
        implicit
        ev0: MergeWith[L, L2, fn.type]
    ): TypedRow[ev0.Out] = {

      val result = repr.mergeWith(that.repr)(fn)(ev0)
      TypedRow.ofTuple(result)
    }
  }

  object mergeKeepRight extends MergeWithFn {

    object fn extends Poly2 {

      implicit def only[T, U]: Case.Aux[T, U, U] = at[T, U] { (_, r) =>
        r
      }
    }
  }
  def ++< : mergeKeepRight.type = mergeKeepRight

  object mergeKeepLeft extends MergeWithFn {

    object fn extends Poly2 {

      implicit def only[T, U]: Case.Aux[T, U, T] = at[T, U] { (l, _) =>
        l
      }
    }
  }
  def >++ : mergeKeepLeft.type = mergeKeepLeft

}

object TypedRow extends TypedRowOrdering.Default.Implicits {

  import shapeless.ops.record._

  case class DynamicProductAPI[T <: Tuple](internal: TypedRow[T]) extends Dynamic {

    def fields: internal.fields.type = internal.fields

    /**
      * Allows dynamic-style access to fields of the record whose keys are Symbols. See
      * [[shapeless.syntax.DynamicRecordOps[_]] for original version
      */
    def selectDynamic(key: String with Singleton)(
        implicit
        selector: Selector[T, Col[key.type]]
    ): selector.Out = {

      fields.selectDynamic(key).value
    }
  }

  object ofNamedArgs extends RecordArgs {

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

  trait FromAny extends Hom.Poly {

    implicit def noOP[L <: Tuple]: TypedRow[L] =>> TypedRow[L] = at[TypedRow[L]] {
      identity[TypedRow[L]] _
    }
  }

  // can this be replaced by a
  object FromAny extends FromAny {

    implicit def fromValue[V]: V =>> TypedRow[Col_->>["value", V] *: Tuple.Empty] = at[V] { v =>
      ofTuple(Col_->>["value"](v) *: Tuple.Empty)
    }
  }
}
