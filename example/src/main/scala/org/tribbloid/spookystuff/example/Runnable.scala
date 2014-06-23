package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by peng on 22/06/14.
 */
trait Runnable {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MoreLinkedIn")
    val sc = new SparkContext(conf)

    doMain()

    sc.stop()
  }

  def doMain()
}
