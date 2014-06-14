package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD
import java.util

/**
 * Created by peng on 12/06/14.
 */
class StringRDDFunctions(val self: RDD[String]) {

  //csv has to be headerless, there is no better solution as header will be shuffled to nowhere
  def csvToMap(headerRow: String, splitter: String = ","): RDD[util.Map[String,String]] = {
    val headers = headerRow.split(splitter)
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

  def tsvToMap(headerRow: String) = csvToMap(headerRow,"\t")
}
