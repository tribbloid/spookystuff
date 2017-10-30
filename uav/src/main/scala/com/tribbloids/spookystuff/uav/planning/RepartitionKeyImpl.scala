package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.row.{BeaconRDD, DataRowSchema}
import com.tribbloids.spookystuff.utils.locality.LocalityRDDView
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * only need to defined a key repartitioning function
  * @tparam K
  */
abstract class RepartitionKeyImpl[K](implicit val ctg: ClassTag[K]) extends GenPartitioner.Instance[K] {

  def schema: DataRowSchema

  def reduceByKey[V: ClassTag](
                                rdd: RDD[(K, V)],
                                reducer: (V, V) => V,
                                beaconRDDOpt: Option[BeaconRDD[K]] = None
                              ): RDD[(K, V)] = {

    val ec = schema.ec
    ec.scratchRDDs.persist(rdd, ec.spooky.spookyConf.defaultStorageLevel) //TODO: optional?
    val keys = rdd.keys

    val keysRepartitioned = repartitionKey(keys, beaconRDDOpt)

    val result = LocalityRDDView(keysRepartitioned).cogroupBase(rdd)
      .values
      .map {
        tuple =>
          tuple._1 -> tuple._2.reduce(reducer)
      }
    result
  }

  def repartitionKey(
                      rdd: RDD[K],
                      beaconRDDOpt: Option[BeaconRDD[K]] = None
                    ): RDD[(K, K)]
}
