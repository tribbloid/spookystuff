package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitionerLike.Instance
import com.tribbloids.spookystuff.row.{BeaconRDD, DataRowSchema}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.language.existentials
import scala.reflect.ClassTag

/**
  * Created by peng on 1/27/15.
  */
//TODO: name should be 'planner'?
sealed trait GenPartitionerLike[+C] {

  def getInstance[K >: C: ClassTag](schema: DataRowSchema): Instance[K]
}

object GenPartitionerLike {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  trait Instance[K] extends Serializable {
    implicit def ctg: ClassTag[K]

    final def createBeaconRDD(
                               ref: RDD[_]
                             ): Option[BeaconRDD[K]] = {

      val result: Option[BeaconRDD[K]] = _createBeaconRDD(ref)
      result.foreach {
        rdd =>
          rdd.assertIsBeacon()
      }
      result
    }

    def _createBeaconRDD(
                          ref: RDD[_]
                        ): Option[BeaconRDD[K]] = None

    //TODO: comparing to old implementation, does this create too much object overhead?
    def groupByKey[V: ClassTag](
                                 rdd: RDD[(K, V)],
                                 beaconRDDOpt: Option[BeaconRDD[K]] = None
                               ): RDD[(K, Iterable[V])] = {
      val itrRDD = rdd.mapValues(v => Iterable(v))
      reduceByKey(itrRDD, {
        _ ++ _
      },
        beaconRDDOpt)
    }

    def reduceByKey[V: ClassTag](
                                  rdd: RDD[(K, V)],
                                  reducer: (V, V) => V,
                                  beaconRDDOpt: Option[BeaconRDD[K]] = None
                                ): RDD[(K, V)]

    //      groupByKey(rdd, beaconRDDOpt)
    //        .map(
    //          tuple =>
    //            tuple._1 -> tuple._2.reduce(reducer)
    //        )
    //    }
  }
}

trait GenPartitioner extends GenPartitionerLike[TraceView]

object GenPartitioners {

  //this won't merge identical traces and do lookup, only used in case each resolve may yield different result
  case object Narrow extends GenPartitioner {

    def getInstance[K: ClassTag](schema: DataRowSchema): Instance[K] = {
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
              .groupBy(_._1)
              .map(v => v._1 -> v._2.map(_._2).reduce(reducer))
              .iterator
        }
      }
    }
  }

  case class Wide(
                   partitionerFactory: RDD[_] => Partitioner = {
                     PartitionerFactories.SamePartitioner
                   }) extends GenPartitioner {

    def getInstance[K: ClassTag](schema: DataRowSchema): Instance[K] = {
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
                          ) extends GenPartitioner {

    def getInstance[K: ClassTag](schema: DataRowSchema): Instance[K] = {
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
