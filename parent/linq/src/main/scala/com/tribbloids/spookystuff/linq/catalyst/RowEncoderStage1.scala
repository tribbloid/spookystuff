package com.tribbloids.spookystuff.linq.catalyst

import ai.acyclic.prover.commons.compat.TupleX
import frameless.{NewInstanceExprs, RecordEncoderFields}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, GetStructField, Literal}
import shapeless.Lazy

case class RowEncoderStage1[G <: TupleX, H <: TupleX](
)(
    implicit
//    i1: DropUnitValues.Aux[G, H],
//    i2: IsHCons[H],
    val fields: Lazy[RecordEncoderFields[H]],
    val newInstanceExprs: Lazy[NewInstanceExprs[G]]
) {

  def cellsToCatalyst(valueExprs: Seq[Expression]): Expression = {
    val nameExprs = fields.value.value.map(field => Literal(field.name))

    // the way exprs are encoded in CreateNamedStruct
    val exprs = nameExprs.zip(valueExprs).flatMap {
      case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
    }

    val createExpr = CreateNamedStruct(exprs)
    createExpr
  }

  def fromCatalystToCells(path: Expression): Seq[Expression] = {
    val exprs = fields.value.value.map { field =>
      field.encoder.fromCatalyst(
        GetStructField(path, field.ordinal, Some(field.name))
      )
    }

    val newArgs = newInstanceExprs.value.from(exprs)
    newArgs
  }
}

object RowEncoderStage1 {

  implicit def usingDerivation[G <: TupleX, H <: TupleX](
      implicit
      i3: Lazy[RecordEncoderFields[H]],
      i4: Lazy[NewInstanceExprs[G]]
  ): RowEncoderStage1[G, H] = RowEncoderStage1[G, H]()
}
