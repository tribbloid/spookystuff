package com.tribbloids.spookystuff.utils.locality

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * result of both cogroupBase & join should preserve locality of partitions of rdd1
  * @tparam K
  * @tparam V
  */
trait LocalityImpl[K, V] extends Product with Serializable {

  def rdd1: RDD[(K, V)]

//  def assertPersisted(rdd: RDD[_]): Unit = {
//
//    assert(rdd1.getStorageLevel != StorageLevel.NONE,
//      "rdd must be persisted")
//  }

  /**
    * ALWAYS LEFT-OUTER! if rdd2 has K2 but rdd1 doesn't, then K2 is lost.
    * @param rdd2
    * @tparam V2
    * @return
    */
  def cogroupBase[V2: ClassTag](
      rdd2: RDD[(K, V2)]
  ): RDD[(K, (V, Iterable[V2]))]

  def join[V2: ClassTag](
      rdd2: RDD[(K, V2)]
  )(
      implicit ev: ClassTag[K]
  ): RDD[(K, (V, V2))] = {

    val base: RDD[(K, (V, Iterable[V2]))] = cogroupBase(rdd2)
    base.flatMapValues { tuple =>
      tuple._2.map { v =>
        tuple._1 -> v
      }
    }
  }
}

/**
  * result of any function should preserve locality of partitions AND
  * ordering of elements inside each partition of rdd1
  * @tparam K
  * @tparam V
  */
trait Locality_OrdinalityImpl[K, V] extends LocalityImpl[K, V]
