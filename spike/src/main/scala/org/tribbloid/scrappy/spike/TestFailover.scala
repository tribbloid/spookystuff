package org.tribbloid.scrappy.spike

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by peng on 05/06/14.
 */
object TestFailover {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    conf.setMaster("local[8,20000]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setJars(SparkContext.jarOfClass(this.getClass).toList)
    conf.set("spark.task.maxFailures","20000")
    val sc = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 8
    val n = 100000 * slices
    val count = sc.parallelize(1 to n, slices).map { i =>
      val x = java.lang.Math.random()
      if (x > 0.9) throw new IllegalStateException("the map has a chance of 10% to fail")
      x
    }.reduce(_ + _)
    sc.stop()
    println("finished")
  }

}
