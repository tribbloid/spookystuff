package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.PreDef
import ai.acyclic.prover.commons.util.Capabilities
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}
import shapeless.ops.hlist.{Mapper, Prepend}
import shapeless.ops.record.{MapValues, RemoveAll, Remover, Selector}
import shapeless.tag.@@
import shapeless.{HList, HNil, ProductArgs, RecordArgs}

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
case class TypedRow[L <: HList](
    cells: ArraySeq[Any]
) extends Dynamic {

  import TypedRow._
  import shapeless.record._

  type Repr = L

  @transient override lazy val toString: String = cells.mkString("[", ",", "]")

  def apply(i: Int): Any = cells.apply(i)

  @transient lazy val asRecord: L = cells
    .foldRight[HList](HNil) { (s, x) =>
      s :: x
    }
    .asInstanceOf[L]

  /**
    * Allows dynamic-style access to fields of the record whose keys are Symbols. See
    * [[shapeless.syntax.DynamicRecordOps[_]] for original version
    */
  def selectDynamic(key: String)(
      implicit
      selector: Selector[L, Symbol @@ key.type]
  ): selector.Out = selector(asRecord)

  def enableOrdering(
      implicit
      ev: MapValues[Caps.AffectOrdering.Enable.asShapeless.type, L]
  ): TypedRow[ev.Out] = {

    val mapped = asRecord.mapValues(Caps.AffectOrdering.Enable)(ev)

    TypedRow.fromHList(mapped)
  }

  def prependAll[L2 <: HList](that: TypedRow[L2])(
      implicit
      prepend: Prepend[L, L2]
  ): TypedRow[prepend.Out] = {

    TypedRow.fromHList(prepend(this.asRecord, that.asRecord))
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

    val newRecord = asRecord.remove(key)
    TypedRow.fromHList(newRecord)
  }

  def removeAll[KS <: HList, L2 <: HList](keys: KS)(
      implicit
      removeAll: RemoveAll.Aux[L, KS, L2]
  ): TypedRow[L2] = {

    val newRecord = removeAll.apply(asRecord)
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

      object Enable extends PreDef.Poly {

        implicit def only[T] = at[T] { v =>
          v.asInstanceOf[T ^^ AffectOrdering]
        }
      }
    }
  }

//  trait OrderingFactory[L <: HList] {
//    def get: Ordering[L]
//  }
//
//  trait OrderingFactory_Imp0 {
//
//    import shapeless.::
//
//    implicit def discard[H, T <: HList](
//        implicit
//        prev: Ordering[T],
//        neo: Ordering[H]
//    ) = {
//
//      new OrderingFactory[H :: T] {
//        override def get = {
//
//          Ordering
//            .by { v: (H :: T) =>
//              v.tail
//            }(prev)
//        }
//      }
//    }
//  }

//  object OrderingFactory {
//
//    import shapeless.::
//
//    implicit def empty: OrderingFactory[HNil] = {
//
//      new OrderingFactory[HNil] {
//        Ordering
//
//      }
//
//      new Ordering[HNil] {
//
//        override def compare(x: HNil, y: HNil): Int = 0
//      }
//    }
//
//    implicit def factor[H, T <: HList](
//        implicit
//        prev: Ordering[T],
//        neo: Ordering[H]
//    ): Ordering[(H @@ TypedRow.Caps.AffectOrdering) :: T] = {
//
//      Ordering
//        .by { v: ((H @@ Caps.AffectOrdering) :: T) =>
//          v.tail
//        }(prev)
//        .orElseBy { v: (H :: T) =>
//          v.head
//        }(neo)
//    }
//  }

//  case class DecideOrdering[T](ordering: Option[Ordering[T]]) {}
//
//  trait DecideOrdering_Imp0 {
//
//    def cannot = DecideOrdering(None)
//  }
//
//  object DecideOrdering extends DecideOrdering_Imp0 {
//
//    import Caps._
//
//    def can[T <: _ @@ AffectOrdering](
//        implicit
//        ev: Ordering[T]
//    ) = DecideOrdering(Some(ev))
//
//  }

  object For {

    trait NativeOrderingBy_Imp0 extends PreDef.Poly {

      implicit def ignore[T]: T =>> Unit = at[T] { v: T =>
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
        val mapped = mapper.apply(row.asRecord)

        mapped
      }

      def get(
          implicit
          hlistOrdering: Ordering[MO]
      ): Ordering[TypedRow[R]] = {

        Ordering.by(fn)
      }
    }

//    implicit def nativeOrdering[
//        V <: HList,
//        V2 <: HList
//    ](
//        implicit
//        ev: Values.Aux[R, V],
//        partition: Partition.Aux[V, Caps.AffectOrdering, V2, _],
//        hlistOrdering: Ordering[V2]
//    ): Ordering[TypedRow[R]] = {
//      // to use it you have to import syntax.hlist.*
//
//      import shapeless.record._
//
//      Ordering.by { row: TypedRow[R] =>
//        val values = row.asRecord.values
//
//        val filtered = values.filter[Caps.AffectOrdering]
//
//        filtered
//      }
//    }
  }

}
