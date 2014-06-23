package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by peng on 22/06/14.
  */
trait RunnableLocal {

  var conf: SparkConf = null
  var sc: SparkContext = null

  final def main(args: Array[String]) {
    conf = new SparkConf().setAppName(this.getClass.getName)
    conf.setMaster("local[*]")
    sc = new SparkContext(conf)

    doMain()

    sc.stop()
  }

  def doMain()
 }
