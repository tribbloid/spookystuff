package com.tribbloids.spookystuff.utils.locality

import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator

import scala.reflect.ClassTag

case class LocalityRDDView[K: ClassTag, V: ClassTag](
    rdd1: RDD[(K, V)],
    persistFn: RDD[_] => Unit = _.persist(),
    isOrdinal: Boolean = true
) extends LocalityImpl[K, V] {

  final val BROADCAST_SIZE_CAP_BYTE = 10 * 1024 * 1024

  override def cogroupBase[V2: ClassTag](rdd2: RDD[(K, V2)]) = {
    lazy val size2 = SizeEstimator.estimate(rdd2)

    val impl: Locality_OrdinalityImpl[K, V] = {
//      if (size2 < BROADCAST_SIZE_CAP_BYTE) BroadcastLocalityImpl(rdd1)
//      else //TODO: enable after testing.
      if (rdd1.partitioner.nonEmpty) SortingLocalityImpl(rdd1)
      else IndexingLocalityImpl(rdd1, persistFn)
    }

    impl.cogroupBase(rdd2)
  }
}
