package org.tribbloid.spookystuff.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.tribbloid.spookystuff.SpookyContext

/**
 * Created by peng on 22/06/14.
 */
trait SparkSubmittable {

  var conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
  var sc: SparkContext = new SparkContext(conf)
  val sql: SQLContext = new SQLContext(sc)
  val spooky: SpookyContext = new SpookyContext(sql)

  final def main(args: Array[String]) {

    val result = doMain()

    result.collect().foreach(println)

    sc.stop()
  }

  def doMain(): RDD[_]
}
