package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.util.Capabilities
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}
import shapeless.ops.hlist.{Partition, Prepend}
import shapeless.ops.record._
import shapeless.tag.@@
import shapeless.{HList, HNil, Poly1, ProductArgs, RecordArgs}

import scala.collection.immutable.ArraySeq
import scala.language.{dynamics, implicitConversions}
import scala.reflect.ClassTag

/**
  * shapeless HList & Record has high runtime overhead and poor Scala 3 compatibility. its usage should be minimized
  *
  * do not use shapeless instances for data storage/shipping
  *
  * @param cells
  *   data
  * @tparam L
  *   Record type
  */
case class TypedRow[L <: HList](cells: ArraySeq[Any]) extends Dynamic {

  import TypedRow.Caps._
  import shapeless.record._

  @transient override lazy val toString: String = cells.mkString("[", ",", "]")

  def apply(i: Int): Any = cells.apply(i)

  @transient lazy val asRecord: L =
    cells.foldRight[HList](HNil)((s, x) => s :: x).asInstanceOf[L]

  /**
    * Allows dynamic-style access to fields of the record whose keys are Symbols. See
    * [[shapeless.syntax.DynamicRecordOps[_]] for original version
    */
  def selectDynamic(
      key: String
  )(
      implicit
      selector: Selector[L, Symbol @@ key.type]
  ): selector.Out = selector(asRecord)

  def enableOrdering(
      implicit
      ev: MapValues[AffectOrdering.Enable.type, L]
  ): TypedRow[ev.Out] = {

    val mapped = asRecord.mapValues(AffectOrdering.Enable)(ev)

    TypedRow.fromHList(mapped)
  }

  def prependAll[L2 <: HList](
      that: TypedRow[L2]
  )(
      implicit
      prepend: Prepend[L, L2]
  ): TypedRow[prepend.Out] = {

    TypedRow.fromHList(prepend(this.asRecord, that.asRecord))
  }

  def ++[L2 <: HList](
      that: TypedRow[L2]
  )(
      implicit
      prepend: Prepend[L, L2]
  ): TypedRow[prepend.Out] = prependAll(that)(prepend)

  // DO NOT define ++: & :++ as the direction of induction is highly subjective

  def remove1[K, L2 <: HList](
      key: K
  )(
      implicit
      remover: Remover.Aux[L, key.type, L2]
  ): TypedRow[L2] = {

    val newRecord = asRecord.remove(key)
    TypedRow.fromHList(newRecord)
  }

  def removeAll[KS <: HList, L2 <: HList](
      keys: KS
  )(
      implicit
      removeAll: RemoveAll.Aux[L, KS, L2]
  ): TypedRow[L2] = {

    val newRecord = removeAll.apply(asRecord)
    TypedRow.fromHList(newRecord)
  }
}

object TypedRow {

  object ofRecord extends RecordArgs {

    def applyRecord[L <: HList](list: L): TypedRow[L] = fromHList(list)
  }

  object ofTuple extends ProductArgs {

    def applyProduct[L <: HList](list: L): TypedRow[L] = fromHList(list)
  }

  def fromHList[L <: HList](
      record: L
  ): TypedRow[L] = {

    val cells = record.runtimeList

    new TypedRow[L](cells.to(ArraySeq))
  }

  def fromValues(values: Any*): TypedRow[HList] = {

    val row = values.to(ArraySeq)
    new TypedRow[HList](row)
  }

  case class WithCatalystTypes(schema: Seq[DataType]) {

    def fromInternalRow(row: InternalRow): TypedRow[HList] = {
      val data = row.toSeq(schema)

      fromValues(data: _*)
    }
  }

  object WithCatalystTypes {}

  lazy val catalystType: ObjectType = ObjectType(classOf[TypedRow[_]])

  implicit def getEncoder[G <: HList](
      implicit
      stage1: RecordEncoderStage1[G, G],
      classTag: ClassTag[TypedRow[G]]
  ): TypedEncoder[TypedRow[G]] = RecordEncoder.ForTypedRow[G, G]()

  object Caps extends Capabilities {

    trait AffectOrdering extends Cap

    object AffectOrdering {

      object Enable extends Poly1 {

        implicit def only[T] = at[T] { v =>
          v.asInstanceOf[T ^^ AffectOrdering]
        }
      }
    }
  }

  case class NativeOrderingResolving[R <: HList]() {

    import Caps._
    import shapeless.record._

    case class Resolve[V <: HList, V2 <: HList](
    )(
        implicit
        ev: Values.Aux[R, V],
        partition: Partition.Aux[V, AffectOrdering, V2, _]
    ) {
      // to use it you have to import syntax.hlist.*

      lazy val fn: TypedRow[R] => V2 = { row: TypedRow[R] =>
        val values = row.asRecord.values

        val filtered = values.filter[AffectOrdering]

        filtered
      }

      def ordering(
          implicit
          hlistOrdering: Ordering[V2]
      ): Ordering[TypedRow[R]] = {

        Ordering.by(fn)
      }
    }
  }

}
