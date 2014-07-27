package org.tribbloid.spookystuff.spike


import org.apache.spark.{SparkContext, SparkConf}
import SparkContext._

/**
 * Created by peng on 05/06/14.
 */
object TestAccumulator {
  def random: Double = java.lang.Math.random()

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    //    conf.setMaster("local-cluster[2,4,1000]") //no can do! spark cannot find jars
    conf.setMaster("local[2,3]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val jars = SparkContext.jarOfClass(this.getClass).toList
    conf.setJars(jars)
    val sc = new SparkContext(conf)
    val acc = sc.accumulator(0)
    val acc2 = sc.accumulator(0)
    val slices = if (args.length > 0) args(0).toInt else 8
    val n = 10000000 * slices
    val count = sc.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      acc += 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce{
      acc2 += 1
      _ + _
    }
    println("Pi is roughly " + 4.0 * count / n)
    println("Map function is used " + acc.value + " times")
    println("Reduce function is used " + acc2.value + " times")
    sc.stop()
    println("finished")
  }

}
