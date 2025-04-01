package com.tribbloids.spookystuff.linq.catalyst

import ai.acyclic.prover.commons.compat.TupleX
import com.tribbloids.spookystuff.linq.Record
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}

case object RowAdapter {

  // DO NOT RENAME! used by reflection-based Catalyst Encoder
  def valueAtIndex[T <: TupleX](tt: Record[T], i: Int): Any = {

    tt._internal.runtimeVector.apply(i)
  }

  case class WithDataTypes(schema: Seq[DataType]) {

    // DO NOT RENAME! used by reflection-based Catalyst Encoder
    def fromInternalRow(row: InternalRow): Record[TupleX] = {
      val data = row.toSeq(schema)

      val vec = data.to(Vector)
      new Record[TupleX](vec)
    }
  }

  lazy val dataType: ObjectType = ObjectType(classOf[Record[?]])
}
