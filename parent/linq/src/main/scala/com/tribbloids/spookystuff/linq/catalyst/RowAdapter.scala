package com.tribbloids.spookystuff.linq.catalyst

import com.tribbloids.spookystuff.linq.Linq.Row
import com.tribbloids.spookystuff.linq.Tuple
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}

case object RowAdapter {

  // DO NOT RENAME! used by reflection-based Catalyst Encoder
  def valueAtIndex[T <: Tuple](tt: Row[T], i: Int): Any = {

    tt._internal.runtimeVector.apply(i)
  }

  case class WithDataTypes(schema: Seq[DataType]) {

    // DO NOT RENAME! used by reflection-based Catalyst Encoder
    def fromInternalRow(row: InternalRow): Row[Tuple] = {
      val data = row.toSeq(schema)

      val vec = data.to(Vector)
      new Row[Tuple](vec)
    }
  }

  lazy val dataType: ObjectType = ObjectType(classOf[Row[?]])
}
