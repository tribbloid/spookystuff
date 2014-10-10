package org.tribbloid.spookystuff.integration

import java.util.UUID

import org.apache.spark.sql.{SQLContext, SchemaRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Tag}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.factory.driver.NaiveDriverFactory

import scala.concurrent.duration._

/**
 * Created by peng on 22/06/14.
 * allowing execution as a main object and tested as a test class
 * keep each test as small as possible, by using downsampling & very few iterations
 */
trait TestCore extends FunSuite {

  object Integration extends Tag("Integration")

  lazy val appName = this.getClass.getSimpleName.replace("$","")
  lazy val conf: SparkConf = new SparkConf().setAppName(appName)
    .setMaster("local[*]")

  lazy val sc: SparkContext = new SparkContext(conf)
  lazy val sql: SQLContext = new SQLContext(sc)
  lazy val spooky: SpookyContext = new SpookyContext(
    sql,
    driverFactory = NaiveDriverFactory(loadImages = true),
    pageExpireAfter = 0.milliseconds
  )
  spooky.setRoot("file://"+System.getProperty("user.home")+"/spOOky/"+appName)

  lazy val result = {
    val result = doMain()

    result.persist()

    result
  }

  def doMain(): SchemaRDD

  test("Print query result",Integration) {
    val array = result.collect()

    array.foreach(row => println(row.mkString("\t")))

    println("-------------------returned "+array.length+" rows------------------")

    result.printSchema()
  }

  final def main(args: Array[String]) {
    result.persist()

    result.map(row => row.mkString("\t")).saveAsTextFile("file://"+System.getProperty("user.home")+"/spOOky/"+appName+"/dump"+UUID.randomUUID())

    val array = result.collect()

    array.foreach(row => println(row.mkString("\t")))

    println("-------------------returned "+array.length+" rows------------------")

    println(result.schema.fieldNames.mkString("\t"))
  }
}