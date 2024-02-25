package com.tribbloids.spookystuff.frameless

import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}
import shapeless.ops.record.Selector
import shapeless.tag.@@
import shapeless.{HList, HNil}

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
    cells: Seq[Any]
) extends Dynamic {

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
}

object TypedRow {

  def fromValues(values: Any*): TypedRow[HList] = {

    val row = values
    new TypedRow[HList](row)
  }

  case class WithCatalystTypes(schema: Seq[DataType]) {

    def fromInternalRow(row: InternalRow): TypedRow[HList] = {
      val data = row.toSeq(schema)

      fromValues(data: _*)
    }

  }

  object WithCatalystTypes {}

  def fromHList[T <: HList](
      hlist: T
  ): TypedRow[T] = {

    val cells = hlist.runtimeList

    TypedRow[T](cells)
  }

  lazy val catalystType: ObjectType = ObjectType(classOf[TypedRow[_]])

  implicit def getEncoder[G <: HList](
      implicit
      stage1: RecordEncoderStage1[G, G],
      classTag: ClassTag[TypedRow[G]]
  ): TypedEncoder[TypedRow[G]] = RecordEncoder.ForTypedRow[G, G]()
}
