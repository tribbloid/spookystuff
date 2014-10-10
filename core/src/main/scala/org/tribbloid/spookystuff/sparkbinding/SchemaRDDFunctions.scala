package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.Utils

/**
 * Created by peng on 12/06/14.
 */
//I really don't want to do this but SparkSQL is an alpha component
class SchemaRDDFunctions(val self: SchemaRDD) {

  def toMapRDD: RDD[Map[String,Any]] = {
    val headers = self.schema.fieldNames

    self.map{
      row => Map(headers.zip(row): _*)
    }
  }

  def toJsonRDD: RDD[String] = this.toMapRDD.map(map => Utils.toJson(map))
}
