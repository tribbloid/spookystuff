package org.tribbloid.spookystuff.driver

import scala.math.random

import org.apache.spark._
/**
 * Created by peng on 04/06/14.
 */
object Driver {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
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
  }
}
