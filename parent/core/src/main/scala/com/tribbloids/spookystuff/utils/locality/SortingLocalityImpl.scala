package com.tribbloids.spookystuff.utils.locality

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class SortingLocalityImpl[K: ClassTag, V: ClassTag](
    override val rdd1: RDD[(K, V)]
) extends LocalityImpl.Ordinality[K, V] {

  {
    assert(rdd1.partitioner.nonEmpty, s"cannot use ${this.getClass.getCanonicalName} on RDD without partitioner")
  }

  lazy val beacon: RDD[(K, (V, Long))] = {

    val acc = rdd1.sparkContext.longAccumulator

    val withLocalIndex = rdd1.mapValues { v =>
      val accV = acc.value
      val result = v -> accV.longValue
      acc.add(1L)
      result
    }
    assert(withLocalIndex.partitioner.nonEmpty)
    withLocalIndex
  }

  override def cogroupBase[V2: ClassTag](rdd2: RDD[(K, V2)]): RDD[(K, (V, Iterable[V2]))] = {

    val cogrouped: RDD[(K, (Iterable[(V, Long)], Iterable[V2]))] = beacon.cogroup(rdd2)
    val flatten: RDD[(K, (Long, V, Iterable[V2]))] = cogrouped.flatMapValues { tuple =>
      tuple._1.map { t =>
        (t._2, t._1, tuple._2)
      }
    }
    val result = flatten
      .mapPartitions { itr =>
        itr.toSeq
          .sortBy(_._2._1)
          .iterator // TODO: may cause OOM! optimize later
          .map {
            case (k, v) =>
              k -> (v._2 -> v._3)
          }
      }
    result
  }
}
