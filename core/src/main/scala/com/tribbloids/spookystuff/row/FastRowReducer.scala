package com.tribbloids.spookystuff.row

/**
  * fast but may yield duplicated result
  */
object FastRowReducer extends RowReducer {

  override def apply(
      v1: Iterable[DataRow],
      v2: Iterable[DataRow]
  ): Iterable[DataRow] = {
    v1 ++ v2
  }
}
