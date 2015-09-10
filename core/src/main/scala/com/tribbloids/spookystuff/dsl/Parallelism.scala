package com.tribbloids.spookystuff.dsl

import org.apache.spark.rdd.RDD

/**
 * Created by peng on 5/14/15.
 */
object Parallelism {

  case class PerCore(n: Int) extends (RDD[_] => Int) {

    override def apply(rdd: RDD[_]): Int = {
      rdd.sparkContext.defaultParallelism * 8
    }
  }
}
