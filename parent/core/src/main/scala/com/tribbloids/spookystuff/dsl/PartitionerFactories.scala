package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.function.hom.Hom.*
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

/**
  * Created by peng on 5/14/15.
  */
object PartitionerFactories {

  case class PerCore(n: Int) extends (RDD[?] :=> Partitioner) {

    override def apply(rdd: RDD[?]): Partitioner = {
      new HashPartitioner(rdd.sparkContext.defaultParallelism * n)
    }
  }

  case object SameParallelism extends (RDD[?] :=> Partitioner) {

    override def apply(rdd: RDD[?]): Partitioner = {
      new HashPartitioner(rdd.partitions.length)
    }
  }

  case object SamePartitioner extends (RDD[?] :=> Partitioner) {

    override def apply(rdd: RDD[?]): Partitioner = {

      rdd.partitioner.getOrElse {
        new HashPartitioner(rdd.partitions.length)
      }
    }
  }
}
