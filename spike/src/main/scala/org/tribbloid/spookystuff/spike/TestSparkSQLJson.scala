package org.tribbloid.spookystuff.spike

import org.apache.spark.{SparkContext, SparkConf, sql}

import org.apache.spark.sql.SQLContext

/**
 * Created by peng on 12/06/14.
 */

object TestSparkSQLJson {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark SQL")
    conf.setMaster("local[8,3]")

    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sql._

    val rdd = sc.parallelize(1 to 100).map( vvv => "{ \"key name\": "+vvv.toString+", \"value name\": 2, \"beta\": 5, \"able\": 3, \"hotel\": 7 }" )
    val schemaRDD = sql.jsonRDD(rdd).select('beta, 'able, 'hotel)

    schemaRDD.printSchema()
//
//    val schemaRDD2 = schemaRDD.where(Symbol("key name") > 20)
////    val rowRDD = schemaRDD.orderBy('key.asc)
//    val rowRDD = schemaRDD
//    println(rowRDD.schemaString)
//
//    val rows = rowRDD.collect()
//    rows.foreach(println(_))

//      .collect()
//    rows.foreach(println(_))

    sc.stop()
  }
}
