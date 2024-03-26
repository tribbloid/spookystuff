package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.Hom
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}
import shapeless.ops.record.Keys

import scala.collection.immutable.ArraySeq

case class TypedRowInternal[L <: Tuple](repr: L) {

  import shapeless.record._

  type Repr = L

  def keys(
      implicit
      ev: Keys[L]
  ): Keys[L]#Out = repr.keys
}

object TypedRowInternal {

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
    def _fromInternalRow(row: InternalRow): TypedRow[Tuple] = {
      val data = row.toSeq(schema)

      val seq = data.to(ArraySeq)
      new TypedRow[Tuple](seq)
    }
  }

  lazy val catalystType: ObjectType = ObjectType(classOf[TypedRow[_]])

}
