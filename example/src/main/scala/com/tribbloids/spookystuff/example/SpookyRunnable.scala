package com.tribbloids.spookystuff.example

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.rdd.FetchedDataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

trait SpookyRunnable extends SpookyEnv {

  override def finalize(): Unit = {
    sc.stop()
  }

  def doMain(spooky: SpookyContext): Any

  final def main(args: Array[String]): Unit = {

    val spooky = getSpooky(args)
    val result = doMain(spooky)

    val array = result match {
      case df: Dataset[_] =>
        val array = df.rdd.persist().takeSample(withReplacement = false, num = 100)
        df.printSchema()
        println(df.schema.fieldNames.mkString("[", "\t", "]"))
        array
      case pageRowRDD: FetchedDataset =>
        val array = pageRowRDD.rdd.persist().takeSample(withReplacement = false, num = 100)
        println(pageRowRDD.schema)
        array
      case rdd: RDD[_] =>
        rdd.persist().takeSample(withReplacement = false, num = 100)
      case _ => Array(result)
    }

    array.foreach(row => println(row))
    println("-------------------returned " + array.length + " rows------------------")
    println(s"------------------fetched ${spooky.spookyMetrics.pagesFetched.value} pages-----------------")
    println(
      s"------------------${spooky.spookyMetrics.pagesFetchedFromCache.value} pages from web cache-----------------")
  }
}
