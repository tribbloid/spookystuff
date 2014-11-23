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
trait ExampleCore extends FunSuite with BeforeAndAfter {

  object Integration extends Tag("Integration")

  val appName = this.getClass.getSimpleName.replace("$","")
  val sc: SparkContext = {
    var conf: SparkConf = new SparkConf().setAppName(appName)

    var master: String = null
    master = Option(master).getOrElse(conf.getOption("spark.master").orNull)
    master = Option(master).getOrElse(System.getenv("MASTER"))
    master = Option(master).getOrElse("local[4,3]")//fail fast

    new SparkContext(conf)
  }

  override def finalize(): Unit = {
    sc.stop()
  }

  val sql: SQLContext = {

    new SQLContext(sc)
  }

  def localPreviewContext(): SpookyContext ={
    new SpookyContext(
      sql,
      driverFactory = NaiveDriverFactory(loadImages = true),
      pageExpireAfter = 0.milliseconds,
      joinLimit = 2,
      sliceLimit = 3,
      maxExploreDepth = Int.MaxValue //TODO: don't need that much
    )
    .setRoot("file://"+System.getProperty("user.home")+"/spooky-integration/"+appName+"/")
  }

  def fullRunContext(): SpookyContext = {
    new SpookyContext(sql)
  }

  def doMain(spooky: SpookyContext): SchemaRDD

  test("Print query result",Integration) {
    val spooky = localPreviewContext()

    val result = doMain(spooky).persist()

    val array = result.collect()
    array.foreach(row => println(row.mkString("\t")))

    println("-------------------returned "+array.length+" rows------------------")
    println(result.schema.fieldNames.mkString("\t"))
  }

  final def main(args: Array[String]) {

    val spooky = if (!args.contains("--fullrun")) localPreviewContext()
    else fullRunContext()

    val result = doMain(spooky).persist()

    val array = result.collect()
    array.foreach(row => println(row.mkString("\t")))

    println("-------------------returned "+array.length+" rows------------------")
    println(result.schema.fieldNames.mkString("\t"))
  }
}