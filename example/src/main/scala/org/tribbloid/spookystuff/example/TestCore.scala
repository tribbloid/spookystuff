package org.tribbloid.spookystuff.example

import org.apache.spark.sql.{SQLContext, SchemaRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite, Tag}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.factory.driver.NaiveDriverFactory

import scala.concurrent.duration._

/**
 * Created by peng on 22/06/14.
 * allowing execution as a main object and tested as a test class
 * keep each test as small as possible, by using downsampling & very few iterations
 */
trait TestCore extends FunSuite with BeforeAndAfter {

  object Integration extends Tag("Integration")

  val appName = this.getClass.getSimpleName.replace("$","")
  val sc: SparkContext = {
    var conf: SparkConf = new SparkConf().setAppName(appName)

    if (conf.get("spark.master") == null)
      conf = conf.setMaster("local[*]")

    new SparkContext(conf)
  }

  override def finalize(): Unit = {
    sc.stop()
  }

  val sql: SQLContext = {

    new SQLContext(sc)
  }

  val spooky: SpookyContext = new SpookyContext(
    sql
  )

  def previeweMode: Unit = {

    spooky.driverFactory = NaiveDriverFactory(loadImages = true)
    spooky.pageExpireAfter = 0.milliseconds
    spooky.joinLimit = 2
    spooky.sliceLimit = 3
    spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-integration/"+appName+"/")
  }

  def doMain(): SchemaRDD

  before {
    previeweMode
  }

  test("Print query result",Integration) {
    val array = doMain.collect()

    array.foreach(row => println(row.mkString("\t")))

    println("-------------------returned "+array.length+" rows------------------")
    println(doMain.schema.fieldNames.mkString("\t"))
  }

  final def main(args: Array[String]) {

    if (!args.contains("--fullrun")) this.previeweMode

    doMain.persist()

    val array = doMain.collect()

    array.foreach(row => println(row.mkString("\t")))

    println("-------------------returned "+array.length+" rows------------------")
    println(doMain.schema.fieldNames.mkString("\t"))
  }
}