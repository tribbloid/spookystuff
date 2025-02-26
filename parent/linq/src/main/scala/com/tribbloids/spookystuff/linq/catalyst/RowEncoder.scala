package com.tribbloids.spookystuff.linq.catalyst

import ai.acyclic.prover.commons.compat.TupleX
import com.tribbloids.spookystuff.linq.Linq.Row
import frameless.TypedEncoder
import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.types.*

import scala.reflect.ClassTag

abstract class RowEncoder[F, G <: TupleX, H <: TupleX](
    implicit
    stage1: RowEncoderStage1[G, H],
    classTag: ClassTag[F]
) extends TypedEncoder[F] {

  import stage1.*

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

object RowEncoder {

  final private val _VALUE_AT_INDEX = "valueAtIndex"
  final private val _FROM_INTERNAL_ROW = "fromInternalRow"

  lazy val catalystAdapterLit: Literal = Literal.fromObject(RowAdapter)

  case class ^[G <: TupleX, H <: TupleX](
  )(
      implicit
      stage1: RowEncoderStage1[G, H],
      classTag: ClassTag[Row[G]]
  ) extends RowEncoder[Row[G], G, H] {

    import stage1.*

    def toCatalyst(path: Expression): Expression = {

      val valueExprs = fields.value.value.zipWithIndex.map {
        case (field, i) =>
          val fieldPath = Invoke(
            catalystAdapterLit,
            _VALUE_AT_INDEX,
            field.encoder.jvmRepr,
            Seq(path, Literal.create(i, IntegerType))
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

      val partial = RowAdapter.WithDataTypes(newArgs.map(_.dataType))

      val newExpr = Invoke(
        Literal.fromObject(partial),
        _FROM_INTERNAL_ROW,
        RowAdapter.dataType,
        Seq(aggregated)
      )

      val nullExpr = Literal.create(null, jvmRepr)

      If(IsNull(path), nullExpr, newExpr)
    }
  }
}
