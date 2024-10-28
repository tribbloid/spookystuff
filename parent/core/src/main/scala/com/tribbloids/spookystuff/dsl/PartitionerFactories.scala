package com.tribbloids.spookystuff.dsl

import ai.acyclic.prover.commons.function.hom.Hom._
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

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
