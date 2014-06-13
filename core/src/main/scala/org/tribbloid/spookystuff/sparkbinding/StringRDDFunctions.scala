package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import java.util

/**
 * Created by peng on 12/06/14.
 */
class StringRDDFunctions(val self: RDD[String]) {

  //CAUTION! this is an action that will trigger a stage
  def asCSV(headerline: String, splitter: String = ","): RDD[util.Map[String,String]] = {
    val headers = headerline.split(splitter)
    val width = headers.length

    //cannot handle when a row is identical to headerline, but whatever
    self.map {
      str => {
        val values = str.split(splitter)
        val row: util.Map[String,String] = new util.HashMap()

        for (i <- 0 to width-1)
        {
          row.put(headers(i), values(i))
        }
        row
      }
    }
  }

  def asTSV(headerline: String) = asCSV(headerline,"\t")
}
