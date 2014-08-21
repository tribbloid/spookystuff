package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by peng on 22/06/14.
 */
trait SparkRunnable {

  var conf: SparkConf = null
  var sc: SparkContext = null

  final def main(args: Array[String]) {
    conf = new SparkConf().setAppName(this.getClass.getName)
    conf.setMaster("local[8,2]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))

    sc = new SparkContext(conf)

    doMain()

    sc.stop()
  }

  def doMain()
}
