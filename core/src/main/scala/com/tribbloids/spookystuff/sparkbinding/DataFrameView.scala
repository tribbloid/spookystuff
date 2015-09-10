package com.tribbloids.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.ListMap

/**
 * Created by peng on 12/06/14.
 */
class DataFrameView(val self: DataFrame) {

  def toMapRDD: RDD[Map[String,Any]] = {
    val headers = self.schema.fieldNames

    val result: RDD[Map[String,Any]] = self.map{
      row => ListMap(headers.zip(row.toSeq): _*)
    }

    result
  }
}
