package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.execution.NodeKey
import org.apache.spark.rdd.RDD

package object row {

  type SquashedFetchedRDD = RDD[SquashedRow]

  type BeaconRDD[K] = RDD[(K, Unit)]

  // TODO: all the following should use Seq
  type Sampler[T] = Seq[(T, Int)] => Seq[(T, Int)] // with index

  type RowReducer = (Iterable[DataRow], Iterable[DataRow]) => Iterable[DataRow]

  type RowOrdering = Ordering[(NodeKey, Iterable[DataRow])]
}
