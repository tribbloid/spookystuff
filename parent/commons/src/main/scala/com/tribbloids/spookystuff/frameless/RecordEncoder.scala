package com.tribbloids.spookystuff.frameless

import frameless.TypedEncoder
import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.types._
import shapeless._

import scala.reflect.ClassTag

abstract class RecordEncoder[F, G <: HList, H <: HList](
    implicit
    stage1: RecordEncoderStage1[G, H],
    classTag: ClassTag[F]
) extends TypedEncoder[F] {

  import stage1._

  def nullable: Boolean = false

  lazy val jvmRepr: DataType = FramelessInternals.objectTypeFor[F]

  lazy val catalystRepr: DataType = {
    val structFields = fields.value.value.map { field =>
      StructField(
        name = field.name,
        dataType = field.encoder.catalystRepr,
        nullable = field.encoder.nullable,
        metadata = Metadata.empty
      )
    }

    StructType(structFields)
  }

}

object RecordEncoder {

  case class ForTypedRow[G <: HList, H <: HList](
  )(
      implicit
      stage1: RecordEncoderStage1[G, H],
      classTag: ClassTag[TypedRow[G]]
  ) extends RecordEncoder[TypedRow[G], G, H] {

    import stage1._

    final private val _apply = "apply"
    final private val _fromInternalRow = "fromInternalRow"

    def toCatalyst(path: Expression): Expression = {

      val valueExprs = fields.value.value.zipWithIndex.map {
        case (field, i) =>
          val fieldPath = Invoke(
            path,
            _apply,
            field.encoder.jvmRepr,
            Seq(Literal.create(i, IntegerType))
          )
          field.encoder.toCatalyst(fieldPath)
      }

      val createExpr = stage1.cellsToCatalyst(valueExprs)

      val nullExpr = Literal.create(null, createExpr.dataType)

      If(IsNull(path), nullExpr, createExpr)
    }

    def fromCatalyst(path: Expression): Expression = {

      val newArgs = stage1.fromCatalystToCells(path)
      val aggregated = CreateStruct(newArgs)

      val partial = TypedRow.WithCatalystTypes(newArgs.map(_.dataType))

      val newExpr = Invoke(
        Literal.fromObject(partial),
        _fromInternalRow,
        TypedRow.catalystType,
        Seq(aggregated)
      )

      val nullExpr = Literal.create(null, jvmRepr)

      If(IsNull(path), nullExpr, newExpr)
    }
  }
}
