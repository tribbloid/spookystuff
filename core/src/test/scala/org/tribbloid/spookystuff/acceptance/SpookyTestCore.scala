package org.tribbloid.spookystuff.acceptance

import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.factory.{NaiveDriverFactory, HtmlUnitDriverFactory, ChromeDriverFactory, FirefoxDriverFactory}

/**
 * Created by peng on 22/06/14.
 * allowing execution as a main object and tested as a test class
 * keep each test as small as possible, by using downsampling & very few iterations
 */
trait SpookyTestCore extends FunSuite {

  lazy val appName = this.getClass.getName
  lazy val conf: SparkConf = new SparkConf().setAppName(appName)
    .setMaster("local[1,3]")
//    .setSparkHome(System.getenv("SPARK_HOME"))

  lazy val sc: SparkContext = new SparkContext(conf)
  lazy val sql: SQLContext = new SQLContext(sc)
  lazy val spooky: SpookyContext = new SpookyContext(sql, NaiveDriverFactory)
  val result = doMain()

  def doMain(): SchemaRDD

  test("Print query result") {
    result.collect().foreach(println(_))
  }

  final def main(args: Array[String]): Unit = {
    result.foreach(println(_))
  }
}