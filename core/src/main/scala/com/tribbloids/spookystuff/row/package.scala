package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.doc.Fetched
import org.apache.spark.rdd.RDD

/**
 * Created by peng on 2/21/15.
 */
package object row {

  type FetchedRow = (DataRow, Seq[Fetched])

  type SquashedFetchedRDD = RDD[SquashedFetchedRow]

  type Sampler[T] = Iterable[(T, Int)] => Iterable[(T, Int)] //with index

  type RowReducer = (Iterable[DataRow], Iterable[DataRow]) => Iterable[DataRow]

  type RowOrdering = Ordering[Iterable[DataRow]]

  // f(open, visited) => open
  type RowEliminator = (Iterable[DataRow], Iterable[DataRow]) => Iterable[DataRow]
}