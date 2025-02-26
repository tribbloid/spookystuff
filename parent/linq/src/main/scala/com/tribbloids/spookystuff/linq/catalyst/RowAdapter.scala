package com.tribbloids.spookystuff.linq.catalyst

import ai.acyclic.prover.commons.compat.TupleX
import com.tribbloids.spookystuff.linq.Linq.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}

case object RowAdapter {

  // DO NOT RENAME! used by reflection-based Catalyst Encoder
  def valueAtIndex[T <: TupleX](tt: Row[T], i: Int): Any = {

    tt._internal.runtimeVector.apply(i)
  }

  case class WithDataTypes(schema: Seq[DataType]) {

    // DO NOT RENAME! used by reflection-based Catalyst Encoder
    def fromInternalRow(row: InternalRow): Row[TupleX] = {
      val data = row.toSeq(schema)

      val vec = data.to(Vector)
      new Row[TupleX](vec)
    }
  }

  lazy val dataType: ObjectType = ObjectType(classOf[Row[?]])
}
