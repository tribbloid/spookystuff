package org.tribbloid.spookystuff

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by peng on 22/06/14.
 */
trait SparkSubmittable {

  var conf: SparkConf = null
  var sc: SparkContext = null

  final def main(args: Array[String]) {
    conf = new SparkConf().setAppName(this.getClass.getName)
    sc = new SparkContext(conf)

    doMain()

    sc.stop()
  }

  def doMain()
}
