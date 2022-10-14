//package com.tribbloids.spookystuff.utils
//
//import org.apache.spark.NarrowDependency
//import org.apache.spark.rdd.RDD
//
//case class LocalShuffleRDD[T](
//    parent: RDD[T]
//) extends RDD[T](parent) {}
//
//object LocalShuffleRDD {
//
//  case class Dependency[T](override val rdd: RDD[T]) extends NarrowDependency[T](rdd) {
//
//    val rddPartitions = rdd.partitions
//
//    override def getParents(partitionId: Int): Seq[Int] = {
//
//      rddPartitions.map {
//        p =>
//          rdd.
//      }
//    }
//  }
//}
