package org.tribbloid.scrappy.spike

import org.apache.spark.{SparkContext, SparkConf, sql}

import org.apache.spark.sql.SQLContext

/**
 * Created by peng on 12/06/14.
 */
case class Record(key: Int, value: String)

object TestSparkSQL {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    //    conf.setMaster("local-cluster[2,4,1000]") //no can do! spark cannot find jars
    conf.setMaster("local[8,3]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val jars = SparkContext.jarOfClass(this.getClass).toList
    conf.setJars(jars)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext._

    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))

    val schemaRDD = rdd.where('key > 1)
    val rows = schemaRDD.orderBy('key.asc).collect()
    rows.foreach(println)
  }
}
