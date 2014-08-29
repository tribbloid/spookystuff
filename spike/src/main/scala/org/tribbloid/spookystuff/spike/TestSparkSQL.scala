package org.tribbloid.spookystuff.spike

import org.apache.spark.{SparkContext, SparkConf, sql}

import org.apache.spark.sql.SQLContext

/**
 * Created by peng on 12/06/14.
 */
case class Record(key: Int, value: String)

object TestSparkSQL {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark SQL")
    //    conf.setMaster("local-cluster[2,4,1000]") //no can do! spark cannot find jars
    conf.setMaster("local[8,3]")
//    conf.setSparkHome(System.getenv("SPARK_HOME"))

    val sc = new SparkContext(conf)

    val sql = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sql._

    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_${200-i}")))

    val schemaRDD = rdd.where('key > 20)
    val rowRDD = schemaRDD.orderBy('key.asc)
//    rowRDD.baseSchemaRDD.sche
    println(rowRDD.schemaString)

    val rows = rowRDD.collect()
    rows.foreach(println(_))

//      .collect()
//    rows.foreach(println(_))

    sc.stop()
  }
}
