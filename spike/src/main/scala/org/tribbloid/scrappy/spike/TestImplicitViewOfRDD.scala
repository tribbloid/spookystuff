package org.tribbloid.scrappy.spike


import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by peng on 05/06/14.
 */
object TestImplicitViewOfRDD {
  def random: Double = java.lang.Math.random()

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    conf.setMaster("local[*]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setJars(SparkContext.jarOfClass(this.getClass).toList)
    val sc = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = sc.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    sc.stop()
    println("finished")
  }

}
