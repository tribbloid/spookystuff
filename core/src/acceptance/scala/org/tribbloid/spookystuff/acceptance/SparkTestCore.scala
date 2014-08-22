package org.tribbloid.spookystuff.acceptance

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, FreeSpec}

/**
 * Created by peng on 22/06/14.
 * allowing execution as a main object and tested as a test class
 * keep each test as small as possible, by using downsampling & very few iterations
 */
trait SparkTestCore extends FunSuite {

  lazy val appName = this.getClass.getName
  lazy val conf: SparkConf = new SparkConf().setAppName(appName)
    .setMaster("local[8,3]")
    .setSparkHome(System.getenv("SPARK_HOME"))
  lazy val sc: SparkContext = new SparkContext(conf)

  val result = doMain()

  def doMain(): Array[_]

  test("Print query result") {
    result.foreach(println(_))
  }

  final def main(args: Array[String]): Unit = {
    result.foreach(println(_))
  }
}