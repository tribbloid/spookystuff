package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD

import scala.collection.immutable.ListMap

/**
 * Created by peng on 12/06/14.
 */
class DataFrameView(val self: SchemaRDD) {

  def asMapRDD: RDD[Map[String,Any]] = {
    val headers = self.schema.fieldNames

    self.map{
      row => ListMap(headers.zip(row): _*)
    }
  }

  //SchemaRDD.toJson already exist
//  def asJsonRDD: RDD[String] = this.asMapRDD.map(map => Utils.toJson(map))
}
