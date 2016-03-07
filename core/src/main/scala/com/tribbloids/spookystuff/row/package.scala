package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.pages.Fetched
import org.apache.spark.rdd.RDD

/**
 * Created by peng on 2/21/15.
 */
package object row {

  type PageRow = (DataRow, Seq[Fetched])

  type SquashedRowRDD = RDD[SquashedPageRow]

  type Sampler[T] = Iterable[(T, Int)] => Iterable[(T, Int)] //with index

  type RowReducer = (Iterable[DataRow], Iterable[DataRow]) => Iterable[DataRow]

  type RowOrdering = Ordering[Iterable[DataRow]]

  // f(open, visited) => open
  type RowEliminator = (Iterable[DataRow], Iterable[DataRow]) => Iterable[DataRow]
}