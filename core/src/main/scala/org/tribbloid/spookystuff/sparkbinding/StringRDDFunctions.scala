package org.tribbloid.spookystuff.sparkbinding

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by peng on 12/06/14.
 */
class StringRDDFunctions(val self: RDD[String]) {

  //csv has to be headerless, there is no better solution as header will be shuffled to nowhere
  def csvToMap(headerRow: String, splitter: String = ","): RDD[Map[String,String]] = {
    val headers = headerRow.split(splitter)
    val width = headers.length

    //cannot handle when a row is identical to headerline, but whatever
    self.map {
      str => {
        val values = str.split(splitter)

        Map(headers.zip(values): _*)
      }
    }
  }

  def tsvToMap(headerRow: String) = csvToMap(headerRow,"\t")
}
