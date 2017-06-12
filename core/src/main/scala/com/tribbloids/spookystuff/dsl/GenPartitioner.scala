package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.dsl.GenPartitioners.Instance
import com.tribbloids.spookystuff.execution.ExecutionPlan
import com.tribbloids.spookystuff.row.BeaconRDD
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.language.existentials
import scala.reflect.ClassTag

/**
  * Created by peng on 1/27/15.
  */
//TODO: name should be 'planner'?
trait GenPartitioner[-C] {

  def getInstance[K <: C: ClassTag](ec: ExecutionPlan.Context): Instance[K]
}

object GenPartitioners {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  trait Instance[K] extends Serializable {
    implicit def ctg: ClassTag[K]

    final def createBeaconRDD(
                               ref: RDD[_]
                             ): Option[BeaconRDD[K]] = {

      val result: Option[BeaconRDD[K]] = _createBeaconRDD(ref)
      result.foreach {
        rdd =>
          rdd.assertIsBeaconRDD()
      }
      result
    }

    def _createBeaconRDD(
                          ref: RDD[_]
                        ): Option[BeaconRDD[K]] = None

    def groupByKey[V: ClassTag](
                                 rdd: RDD[(K, V)],
                                 beaconRDDOpt: Option[BeaconRDD[K]] = None
                               ): RDD[(K, Iterable[V])]

    def reduceByKey[V: ClassTag](
                                  rdd: RDD[(K, V)],
                                  reducer: (V, V) => V,
                                  beaconRDDOpt: Option[BeaconRDD[K]] = None
                                ): RDD[(K, V)] = {

      groupByKey(rdd, beaconRDDOpt)
        .map(
          tuple =>
            tuple._1 -> tuple._2.reduce(reducer)
        )
    }
  }

  //this won't merge identical traces and do lookup, only used in case each resolve may yield different result
  case object Narrow extends GenPartitioner[Any] {

    def getInstance[K: ClassTag](ec: ExecutionPlan.Context): Instance[K] = {
      Inst[K]()
    }

    case class Inst[K](
                        implicit val ctg: ClassTag[K]
                      ) extends Instance[K] {

      override def groupByKey[V: ClassTag](
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

  case class Wide(partitionerFactory: RDD[_] => Partitioner = {
    PartitionerFactories.SamePartitioner
  }) extends GenPartitioner[Any] {

    def getInstance[K: ClassTag](ec: ExecutionPlan.Context): Instance[K] = {
      Inst[K]()
    }

    case class Inst[K](
                        implicit val ctg: ClassTag[K]
                      ) extends Instance[K] {

      // very expensive and may cause memory overflow.
      // TODO: does cogrouping with oneself solve the problem?
      override def groupByKey[V: ClassTag](
                                            rdd: RDD[(K, V)],
                                            beaconRDDOpt: Option[BeaconRDD[K]] = None
                                          ): RDD[(K, Iterable[V])] = {
        val partitioner = partitionerFactory(rdd)
        rdd.groupByKey(partitioner)
      }

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
                          ) extends GenPartitioner[Any] {

    def getInstance[K: ClassTag](ec: ExecutionPlan.Context): Instance[K] = {
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

      override def groupByKey[V: ClassTag](
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

  //case object AutoDetect extends QueryOptimizer
}