package com.tribbloids.spookystuff.linq.catalyst

import ai.acyclic.prover.commons.compat.TupleX
import com.tribbloids.spookystuff.linq.Rec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, ObjectType}

case object RowAdapter {

  // DO NOT RENAME! used by reflection-based Catalyst Encoder
  def valueAtIndex[T <: TupleX](tt: Rec[T], i: Int): Any = {

    tt._internal.runtimeVector.apply(i)
  }

  case class WithDataTypes(schema: Seq[DataType]) {

    // DO NOT RENAME! used by reflection-based Catalyst Encoder
    def fromInternalRow(row: InternalRow): Rec[TupleX] = {
      val data = row.toSeq(schema)

      val vec = data.to(Vector)
      new Rec[TupleX](vec)
    }
  }

  lazy val dataType: ObjectType = ObjectType(classOf[Rec[?]])
}
