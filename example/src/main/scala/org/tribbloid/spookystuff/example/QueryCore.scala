package org.tribbloid.spookystuff.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.tribbloid.spookystuff.SpookyContext

trait QueryCore extends LocalSpookyCore {

  override def finalize(): Unit = {
    sc.stop()
  }

  def doMain(spooky: SpookyContext): Any

  final def main(args: Array[String]) {

    val spooky = getSpooky(args)
    val result = doMain(spooky)

    val rdd: RDD[_] = result match {
      case schemaRdd: DataFrame =>
        println(schemaRdd.schema.fieldNames.mkString("\t"))
        schemaRdd.rdd
      case rdd: RDD[_] => rdd
    }

    val array = rdd.persist().takeSample(withReplacement = false, num = 100)
    array.foreach(row => println(row))
    println("-------------------returned "+array.length+" rows------------------")
    println(s"------------------fetched ${spooky.metrics.pagesFetched.value} pages-----------------")
    println(s"------------------${spooky.metrics.pagesFetchedFromCache.value} pages from web cache-----------------")

    //    rdd.saveAsTextFile("file://"+System.getProperty("user.home")+"/spooky-local/result"+s"/$appName-${System.currentTimeMillis()}.json")
  }
}