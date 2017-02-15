package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.dsl.GenPartitioners.GenPartitionerImpl
import com.tribbloids.spookystuff.row.BeaconRDD
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.language.existentials
import scala.reflect.ClassTag

/**
  * Created by peng on 1/27/15.
  */
trait GenPartitioner {

  def getImpl(spooky: SpookyContext): GenPartitionerImpl
}

object GenPartitioners {

  import RDD._

  trait GenPartitionerImpl extends Serializable {

    final def createBeaconRDD[K: ClassTag](
                                            ref: RDD[_]
                                          ): Option[BeaconRDD[K]] = {
      val result: Option[BeaconRDD[K]] = _createBeaconRDD(ref)
      result.foreach {
        rdd =>
          SpookyUtils.RDDs.assertIsBeaconRDD(rdd)
      }
      result
    }

    def _createBeaconRDD[K: ClassTag](
                                       ref: RDD[_]
                                     ): Option[BeaconRDD[K]] = None

    def groupByKey[K: ClassTag, V: ClassTag](
                                              rdd: RDD[(K, V)],
                                              beaconRDDOpt: Option[BeaconRDD[K]] = None
                                            ): RDD[(K, Iterable[V])]

    // inefficient for wide transformation
    def reduceByKey[K: ClassTag, V: ClassTag](
                                               rdd: RDD[(K, V)],
                                               reducer: (V, V) => V,
                                               beaconRDDOpt: Option[BeaconRDD[K]] = None
                                             ): RDD[(K, V)] = {
      groupByKey[K, V](rdd, beaconRDDOpt)
        .map(
          tuple =>
            tuple._1 -> tuple._2.reduce(reducer)
        )
    }
  }

  //this won't merge identical traces and do lookup, only used in case each resolve may yield different result
  case object Narrow extends GenPartitioner {

    override def getImpl(spooky: SpookyContext): GenPartitionerImpl = GPImpl

    object GPImpl extends GenPartitionerImpl {

      override def groupByKey[K: ClassTag, V: ClassTag](
                                                         rdd: RDD[(K, V)],
                                                         beaconRDDOpt: Option[BeaconRDD[K]] = None
                                                       ): RDD[(K, Iterable[V])] = {
        rdd.mapPartitions{
          itr =>
            itr
              .toTraversable
              .groupBy(_._1)
              .map(v => v._1 -> v._2.map(_._2).toIterable)
              .iterator
        }
      }
    }
  }

  case class Wide(partitionerFactory: RDD[_] => Partitioner = PartitionerFactories.SamePartitioner) extends GenPartitioner {

    override def getImpl(spooky: SpookyContext): GenPartitionerImpl = GPImpl

    object GPImpl extends GenPartitionerImpl {

      override def groupByKey[K: ClassTag, V: ClassTag](
                                                         rdd: RDD[(K, V)],
                                                         beaconRDDOpt: Option[BeaconRDD[K]] = None
                                                       ): RDD[(K, Iterable[V])] = {
        val partitioner = partitionerFactory(rdd)
        rdd.groupByKey(partitioner)
      }

      //this is faster and saves more memory
      override def reduceByKey[K: ClassTag, V: ClassTag](
                                                          rdd: RDD[(K, V)],
                                                          reducer: (V, V) => V,
                                                          beaconRDDOpt: Option[BeaconRDD[K]] = None
                                                        ): RDD[(K, V)] = {
        val partitioner = partitionerFactory(rdd)
        rdd.reduceByKey(partitioner, reducer)
      }
    }
  }

  //group identical ActionPlans, execute in parallel, and duplicate result pages to match their original contexts
  //reduce workload by avoiding repeated access to the same url caused by duplicated context or diamond links (A->B,A->C,B->D,C->D)
  case class DocCacheAware(partitionerFactory: RDD[_] => Partitioner = PartitionerFactories.SamePartitioner) extends GenPartitioner {

    override def getImpl(spooky: SpookyContext): GenPartitionerImpl = GPImpl

    object GPImpl extends GenPartitionerImpl {

      override def _createBeaconRDD[K: ClassTag](
                                                  ref: RDD[_]
                                                ): Option[BeaconRDD[K]] = {

        val partitioner = partitionerFactory(ref)
        val result = ref.sparkContext
          .emptyRDD[(K, Unit)]
          .partitionBy(partitioner)
          .persist(StorageLevel.MEMORY_AND_DISK)
        result.count()
        Some(result)
      }

      override def groupByKey[K: ClassTag, V: ClassTag](
                                                         rdd: RDD[(K, V)],
                                                         beaconRDDOpt: Option[BeaconRDD[K]] = None
                                                       ): RDD[(K, Iterable[V])] = {
        val beaconRDD = beaconRDDOpt.get

        val partitioner = partitionerFactory(rdd)
        val cogrouped = rdd.cogroup(beaconRDD, beaconRDD.partitioner.getOrElse(partitioner))
        cogrouped.mapValues {
          tuple =>
            tuple._1
        }
      }
    }
  }

  //case object Inductive extends QueryOptimizer
  //
  //case object AutoDetect extends QueryOptimizer
}