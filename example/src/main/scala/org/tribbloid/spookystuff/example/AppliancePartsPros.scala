package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by peng on 07/06/14.
 */
object AppliancePartsPros {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    conf.setMaster("local[*]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setJars(SparkContext.jarOfClass(this.getClass).toList)
    val sc = new SparkContext(conf)
  }
}
