package com.tribbloids.spookystuff.row

trait RowReducer extends RowReducer.ReduceFn with Serializable {

  import RowReducer._

  def reduce(
      v1: Rows,
      v2: Rows
  ): Rows

  final override def apply(
      old: Rows,
      neo: Rows
  ): Rows = reduce(old, neo)
}

object RowReducer {

//  implicit def unbox(self: RowReducer): ReduceFn = self.reduce

  type Rows = Vector[DataRow]

  type ReduceFn = (Rows, Rows) => Rows
}
