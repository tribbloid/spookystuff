package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.dsl.DataLocality.Instance
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.row.{BeaconRDD, SpookySchema}
import com.tribbloids.spookystuff.utils.locality.LocalityRDDView
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait DataLocality[
    L, // lower bound
    -U >: L // upper bound
] {

  def getInstance[K >: L <: U: ClassTag](schema: SpookySchema): Instance[K]
}

object DataLocality {

  import com.tribbloids.spookystuff.utils.RDDImplicits.*

  trait Instance[K] extends Serializable {
    implicit def ctg: ClassTag[K]

    final def createBeaconRDD(
        ref: RDD[?]
    ): Option[BeaconRDD[K]] = {

      val result: Option[BeaconRDD[K]] = _createBeaconRDD(ref)
      result.foreach { rdd =>
        rdd.assertIsBeacon()
      }
      result
    }

    def _createBeaconRDD(
        ref: RDD[?]
    ): Option[BeaconRDD[K]] = None

    // TODO: comparing to old implementation, does this create too much object overhead?
    final def groupByKey[V: ClassTag](
        rdd: RDD[(K, V)],
        beaconRDDOpt: Option[BeaconRDD[K]] = None
    ): RDD[(K, Iterable[V])] = {
      val itrRDD = rdd.mapValues(v => Iterable(v))
      reduceByKey(itrRDD, _ ++ _, beaconRDDOpt)
    }

    def reduceByKey[V: ClassTag](
        rdd: RDD[(K, V)],
        reducer: (V, V) => V,
        beaconRDDOpt: Option[BeaconRDD[K]] = None
    ): RDD[(K, V)]
  }

  /**
    * only need to defined a key repartitioning function
    */
  abstract class RepartitionKeyImpl[K](
      implicit
      val ctg: ClassTag[K]
  ) extends Instance[K] {

    def ec: ExecutionContext

    def reduceByKey[V: ClassTag](
        rdd: RDD[(K, V)],
        reducer: (V, V) => V,
        beaconRDDOpt: Option[BeaconRDD[K]] = None
    ): RDD[(K, V)] = {

      ec.persist(rdd) // TODO: optional?
      val keys = rdd.keys

      val keysRepartitioned = repartitionKey(keys, beaconRDDOpt)

      val result = LocalityRDDView(keysRepartitioned)
        .cogroupBase(rdd)
        .values
        .map { tuple =>
          tuple._1 -> tuple._2.reduce(reducer)
        }
      result
    }

    def repartitionKey(
        rdd: RDD[K],
        beaconRDDOpt: Option[BeaconRDD[K]] = None
    ): RDD[(K, K)]
  }

  trait PassThrough extends Locality_Wide {

    def getInstance[K: ClassTag](schema: SpookySchema): Instance[K] = {
      Inst[K]()
    }

    case class Inst[K]()(
        implicit
        val ctg: ClassTag[K]
    ) extends Instance[K] {

      override def reduceByKey[V: ClassTag](
          rdd: RDD[(K, V)],
          reducer: (V, V) => V,
          beaconRDDOpt: Option[BeaconRDD[K]] = None
      ): RDD[(K, V)] = {
        rdd
      }
    }
  }
}
