package org.tribbloid.spookystuff.integration

import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Tag, BeforeAndAfter, FunSuite}
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.dsl.driverfactory.NaiveDriverFactory

/**
 * Created by peng on 12/2/14.
 */
abstract class IntegrationCore extends FunSuite with BeforeAndAfter {

  import scala.concurrent.duration._

  object Integration extends Tag("Integration")

  val appName = this.getClass.getSimpleName.replace("$","")
  val sc: SparkContext = {
    var conf: SparkConf = new SparkConf().setAppName(appName)

    var master: String = null
    master = Option(master).getOrElse(conf.getOption("spark.master").orNull)
    master = Option(master).getOrElse(System.getenv("MASTER"))
    master = Option(master).getOrElse("local[4,3]")//fail fast

    conf.setMaster(master)
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
      maxExploreDepth = 3
    )
      .setRoot("file://"+System.getProperty("user.home")+"/spooky-integration/"+appName+"/")
  }

  def fullRunContext(): SpookyContext = {
    new SpookyContext(sql)
  }

  def doMain(spooky: SpookyContext): SchemaRDD

  test("Print query result") {
    val spooky = localPreviewContext()

    val result = doMain(spooky).persist()

    val array = result.collect()
    array.foreach(row => println(row.mkString("\t")))

    println("-------------------returned "+array.length+" rows------------------")
    println(result.schema.fieldNames.mkString("\t"))
  }
}
