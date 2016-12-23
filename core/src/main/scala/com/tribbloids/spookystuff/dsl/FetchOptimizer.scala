package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.dsl.FetchOptimizers.GenPartitionerImpl
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.language.existentials
import scala.reflect.ClassTag

/**
  * Created by peng on 1/27/15.
  */
sealed trait FetchOptimizer {

  def getImpl(
               partitioner: Partitioner
             ): GenPartitionerImpl
}

object FetchOptimizers {

  import RDD._

  trait GenPartitionerImpl {

    def groupByKey[K: ClassTag, V: ClassTag, W](
                                                 rdd: RDD[(K, V)],
                                                 beaconRDDOpt: Option[RDD[(K, W)]] = None
                                               ): RDD[(K, Iterable[V])]

    // inefficient for wide transformation
    def reduceByKey[K: ClassTag, V: ClassTag, W](
                                                  rdd: RDD[(K, V)],
                                                  reducer: (V, V) => V,
                                                  beaconRDDOpt: Option[RDD[(K, W)]] = None
                                                ): RDD[(K, V)] = {
      groupByKey[K, V, W](rdd, beaconRDDOpt)
        .map(
          tuple =>
            tuple._1 -> tuple._2.reduce(reducer)
        )
    }
  }

  //this won't merge identical traces and do lookup, only used in case each resolve may yield different result
  case object Narrow extends FetchOptimizer {

    override def getImpl(partitioner: Partitioner): GenPartitionerImpl = new GPImpl()

    class GPImpl extends GenPartitionerImpl {

      override def groupByKey[K: ClassTag, V: ClassTag, W](
                                                            rdd: RDD[(K, V)],
                                                            beaconRDDOpt: Option[RDD[(K, W)]] = None
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

  case object Wide extends FetchOptimizer {

    override def getImpl(partitioner: Partitioner): GenPartitionerImpl = new GPImpl(partitioner)

    class GPImpl(partitioner: Partitioner) extends GenPartitionerImpl {

      override def groupByKey[K: ClassTag, V: ClassTag, W](
                                                            rdd: RDD[(K, V)],
                                                            beaconRDDOpt: Option[RDD[(K, W)]] = None
                                                          ): RDD[(K, Iterable[V])] = {
        rdd.groupByKey(partitioner)
      }

      //this is faster and saves more memory
      override def reduceByKey[K: ClassTag, V: ClassTag, W](
                                                             rdd: RDD[(K, V)],
                                                             reducer: (V, V) => V,
                                                             beaconRDDOpt: Option[RDD[(K, W)]] = None
                                                           ): RDD[(K, V)] = {
        rdd.reduceByKey(partitioner, reducer)
      }
    }
  }

  //group identical ActionPlans, execute in parallel, and duplicate result pages to match their original contexts
  //reduce workload by avoiding repeated access to the same url caused by duplicated context or diamond links (A->B,A->C,B->D,C->D)
  case object DocCacheAware extends FetchOptimizer {

    override def getImpl(partitioner: Partitioner): GenPartitionerImpl = new GPImpl(partitioner)

    class GPImpl(partitioner: Partitioner) extends GenPartitionerImpl {

      override def groupByKey[K: ClassTag, V: ClassTag, W](
                                                            rdd: RDD[(K, V)],
                                                            beaconRDDOpt: Option[RDD[(K, W)]] = None
                                                          ): RDD[(K, Iterable[V])] = {
        val beaconRDD = beaconRDDOpt.get

        val cogrouped = rdd.cogroup[W](beaconRDD, beaconRDD.partitioner.getOrElse(partitioner))
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