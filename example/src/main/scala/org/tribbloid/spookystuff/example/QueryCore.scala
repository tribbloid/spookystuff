package org.tribbloid.spookystuff.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.SpookyContext

trait QueryCore extends LocalSpookyCore {

  override def finalize(): Unit = {
    sc.stop()
  }

  def doMain(spooky: SpookyContext): RDD[_]

  final def main(args: Array[String]) {

    val spooky = getSpooky(args)
    val result = doMain(spooky).cache()

    result.saveAsTextFile("file://"+System.getProperty("user.home")+"/spooky-local/result"+s"/$appName-${System.currentTimeMillis()}.json")

    val array = result.collect()

    array.foreach(row => println(row))

    println("-------------------returned "+array.length+" rows------------------")
    result match {
      case schemaRdd: SchemaRDD => println(schemaRdd.schema.fieldNames.mkString("\t"))
      case _ =>
    }
    println(s"------------------fetched ${spooky.metrics.pagesFetched.value} pages-----------------")
  }
}