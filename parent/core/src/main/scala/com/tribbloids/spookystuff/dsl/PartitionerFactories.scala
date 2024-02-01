package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.function.Impl.:=>
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 5/14/15.
  */
object PartitionerFactories {

  case class PerCore(n: Int) extends (RDD[_] :=> Partitioner) {

    override def apply(rdd: RDD[_]): Partitioner = {
      new HashPartitioner(rdd.sparkContext.defaultParallelism * n)
    }
  }

  case object SameParallelism extends (RDD[_] :=> Partitioner) {

    override def apply(rdd: RDD[_]): Partitioner = {
      new HashPartitioner(rdd.partitions.length)
    }
  }

  case object SamePartitioner extends (RDD[_] :=> Partitioner) {

    override def apply(rdd: RDD[_]): Partitioner = {

      rdd.partitioner.getOrElse {
        new HashPartitioner(rdd.partitions.length)
      }
    }
  }
}
