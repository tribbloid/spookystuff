package org.tribbloid.spookystuff.integration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.factory.NaiveDriverFactory

/**
 * Created by peng on 22/06/14.
 * allowing execution as a main object and tested as a test class
 * keep each test as small as possible, by using downsampling & very few iterations
 */
trait SpookyTestCore extends FunSuite {

  lazy val appName = this.getClass.getSimpleName.replace("$","")
  lazy val conf: SparkConf = new SparkConf().setAppName(appName)
    .setMaster("local[8,3]")

  lazy val sc: SparkContext = new SparkContext(conf)
  lazy val sql: SQLContext = new SQLContext(sc)
  lazy val spooky: SpookyContext = new SpookyContext(
    sql,
    driverFactory = NaiveDriverFactory,
    saveRoot = "file:///home/peng/spooky-page/"+appName,
    errorDumpRoot = "file:///home/peng/spooky-error/"+appName,
    localErrorDumpRoot = "temp/spooky-error/"+appName
  )
  lazy val result = doMain()

  def doMain(): RDD[_]

  test("Print query result") {
    result.collect().foreach(println(_))
  }

  final def main(args: Array[String]): Unit = {
    result.collect().foreach(println(_))
  }
}