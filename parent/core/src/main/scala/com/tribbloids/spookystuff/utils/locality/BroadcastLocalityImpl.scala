package com.tribbloids.spookystuff.utils.locality

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class BroadcastLocalityImpl[K: ClassTag, V: ClassTag](
    override val rdd1: RDD[(K, V)]
) extends LocalityImpl.Ordinality[K, V] {

  override def cogroupBase[V2: ClassTag](rdd2: RDD[(K, V2)]): RDD[(K, (V, Iterable[V2]))] = {

    //      val grouped: RDD[(K, Iterable[V])] = self.groupByKey()
    //      val a = grouped.map(_._2.toList).collect()
    val otherMap = rdd2.groupByKey().collectAsMap()
    val otherMap_broadcast = rdd1.sparkContext.broadcast(otherMap)

    val cogrouped: RDD[(K, (V, Iterable[V2]))] = rdd1.mapPartitions { bigItr =>
      val otherMap = otherMap_broadcast.value
      val result = bigItr.map {
        case (k, itr) =>
          val itr2 = otherMap.getOrElse(k, Nil)
          k -> (itr -> itr2)
      }
      result
    }
    cogrouped
  }
}
