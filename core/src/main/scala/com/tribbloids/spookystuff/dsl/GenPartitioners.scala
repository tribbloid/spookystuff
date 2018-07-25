package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.dsl.GenPartitionerLike.Instance
import com.tribbloids.spookystuff.row.{BeaconRDD, SpookySchema}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.language.existentials
import scala.reflect.ClassTag

object GenPartitioners {

  case object Passthrogh extends GenPartitionerLike.Passthrough

  //this won't merge identical traces and do lookup, only used in case each resolve may yield different result
  case object Narrow extends AnyGenPartitioner {

    def getInstance[K: ClassTag](schema: SpookySchema): Instance[K] = {
      Inst[K]()
    }

    case class Inst[K](
                        implicit val ctg: ClassTag[K]
                      ) extends Instance[K] {

      override def reduceByKey[V: ClassTag](
                                             rdd: RDD[(K, V)],
                                             reducer: (V, V) => V,
                                             beaconRDDOpt: Option[BeaconRDD[K]] = None
                                           ): RDD[(K, V)] = {
        rdd.mapPartitions{
          itr =>
            itr
              .toTraversable
              .groupBy(_._1) //TODO: is it memory efficient? Write a test case for it
              .map(v => v._1 -> v._2.map(_._2).reduce(reducer))
              .iterator
        }
      }
    }
  }

  case class Wide(
                   partitionerFactory: RDD[_] => Partitioner = {
                     PartitionerFactories.SamePartitioner
                   }) extends AnyGenPartitioner {

    def getInstance[K: ClassTag](schema: SpookySchema): Instance[K] = {
      Inst[K]()
    }

    case class Inst[K](
                        implicit val ctg: ClassTag[K]
                      ) extends Instance[K] {

      //this is faster and saves more memory
      override def reduceByKey[V: ClassTag](
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
  case class DocCacheAware(
                            partitionerFactory: RDD[_] => Partitioner = {
                              PartitionerFactories.SamePartitioner
                            }
                          ) extends AnyGenPartitioner {

    def getInstance[K: ClassTag](schema: SpookySchema): Instance[K] = {
      Inst[K]()
    }

    case class Inst[K](
                        implicit val ctg: ClassTag[K]
                      ) extends Instance[K] {

      override def _createBeaconRDD(
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

      override def reduceByKey[V: ClassTag](
                                             rdd: RDD[(K, V)],
                                             reducer: (V, V) => V,
                                             beaconRDDOpt: Option[BeaconRDD[K]] = None
                                           ): RDD[(K, V)] = {

        val beaconRDD = beaconRDDOpt.get

        val partitioner = partitionerFactory(rdd)
        val cogrouped = rdd
          .cogroup(beaconRDD, beaconRDD.partitioner.getOrElse(partitioner))
        cogrouped.mapValues {
          tuple =>
            tuple._1.reduce(reducer)
        }
      }
    }
  }

  //case object AutoDetect extends QueryOptimizer
}
