package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.Hom
import ai.acyclic.prover.commons.util.Capabilities
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}
import shapeless.ops.hlist.{Mapper, Prepend}
import shapeless.ops.record.{MapValues, RemoveAll, Remover, Selector}
import shapeless.tag.@@
import shapeless.{HList, HNil, ProductArgs, RecordArgs}

import scala.collection.immutable.ArraySeq
import scala.language.dynamics
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
case class TypedRow[L <: HList](
    cells: ArraySeq[Any]
) {

  import TypedRow._
  import shapeless.record._

  type Repr = L

  @transient override lazy val toString: String = cells.mkString("[", ",", "]")

  // DO NOT MOVE! used by reflection-based Catalyst Encoder
  def getValue(i: Int): Any = cells.apply(i)

  object values extends Dynamic {

    /**
      * Allows dynamic-style access to fields of the record whose keys are Symbols. See
      * [[shapeless.syntax.DynamicRecordOps[_]] for original version
      */
    def selectDynamic(key: String)(
        implicit
        selector: Selector[L, Symbol @@ key.type]
    ): selector.Out = selector(asRepr)
  }

  @transient lazy val asRepr: L = cells
    .foldRight[HList](HNil) { (s, x) =>
      s :: x
    }
    .asInstanceOf[L]

  def enableOrdering(
      implicit
      ev: MapValues[Caps.AffectOrdering.Enable.asShapeless.type, L]
  ): TypedRow[ev.Out] = {

    val mapped = asRepr.mapValues(Caps.AffectOrdering.Enable)(ev)

    TypedRow.fromHList(mapped)
  }

  def prependAll[L2 <: HList](that: TypedRow[L2])(
      implicit
      prepend: Prepend[L, L2]
  ): TypedRow[prepend.Out] = {

    TypedRow.fromHList(prepend(this.asRepr, that.asRepr))
  }

  def ++[L2 <: HList](that: TypedRow[L2])(
      implicit
      prepend: Prepend[L, L2]
  ): TypedRow[prepend.Out] = prependAll(that)(prepend)

  // DO NOT define ++: & :++ as the direction of induction is highly subjective

  def remove1[K, L2 <: HList](key: K)(
      implicit
      remover: Remover.Aux[L, key.type, L2]
  ): TypedRow[L2] = {

    val newRecord = asRepr.remove(key)
    TypedRow.fromHList(newRecord)
  }

  def removeAll[KS <: HList, L2 <: HList](keys: KS)(
      implicit
      removeAll: RemoveAll.Aux[L, KS, L2]
  ): TypedRow[L2] = {

    val newRecord = removeAll.apply(asRepr)
    TypedRow.fromHList(newRecord)
  }
}

object TypedRow extends RecordArgs {

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

      object Enable extends Hom.Poly {

        implicit def only[T] = at[T] { v =>
          v.asInstanceOf[T ^^ AffectOrdering]
        }
      }
    }
  }

  object For {

    trait NativeOrderingBy_Imp0 extends Hom.Poly {

      implicit def ignore[T]: T =>> Unit = at[T] { _: T =>
        ()
      }
    }

    object NativeOrderingBy extends NativeOrderingBy_Imp0 {

      implicit def accept[T <: Caps.^^[_, Caps.AffectOrdering]]: T =>> T = at[T] { v: T =>
        v
      }
    }

    def apply[R <: HList] = new For[R]
  }

  class For[R <: HList] {

    // applicable to all cases of HList, even without KeyTags
    case class NativeOrdering[
        MO <: HList
    ]()(
        implicit
        mapper: Mapper.Aux[For.NativeOrderingBy.asShapeless.type, R, MO]
    ) {
      // to use it you have to import syntax.hlist.*

      type Mapped = MO

      lazy val fn: TypedRow[R] => MO = { row: TypedRow[R] =>
        val mapped = mapper.apply(row.asRepr)

        mapped
      }

      def get(
          implicit
          hlistOrdering: Ordering[MO]
      ): Ordering[TypedRow[R]] = {

        Ordering.by(fn)
      }
    }

  }

}
