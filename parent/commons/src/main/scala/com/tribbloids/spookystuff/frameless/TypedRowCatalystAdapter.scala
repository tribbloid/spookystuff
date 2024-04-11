package com.tribbloids.spookystuff.frameless

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}

import scala.collection.immutable.ArraySeq

case object TypedRowCatalystAdapter {

  // DO NOT RENAME! used by reflection-based Catalyst Encoder
  def valueAtIndex[T <: Tuple](tt: TypedRow[T], i: Int): Any = {

    tt._internal.cells.apply(i)
  }

  case class WithDataTypes(schema: Seq[DataType]) {

    // DO NOT RENAME! used by reflection-based Catalyst Encoder
    def fromInternalRow(row: InternalRow): TypedRow[Tuple] = {
      val data = row.toSeq(schema)

      val seq = data.to(ArraySeq)
      new TypedRow[Tuple](seq)
    }
  }

  lazy val dataType: ObjectType = ObjectType(classOf[TypedRow[_]])
}
