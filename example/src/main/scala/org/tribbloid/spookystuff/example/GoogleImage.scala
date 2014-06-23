package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.Conf
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.SpookyContext._

/**
* Created by peng on 10/06/14.
*/
object GoogleImage {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GoogleImage")
    val sc = new SparkContext(conf)

    //    val nameRDD = sc.textFile("/home/peng/Documents/affiliation.txt").distinct(16)
    (sc.textFile("/home/peng/Documents/affiliation_short5.txt",16) +>
      Visit("http://images.google.com/") +>
      DelayFor("form[action=\"/search\"]",50) +>
      TextInput("input[name=\"q\"]","#{_} Logo") +>
      Submit("input[name=\"btnG\"]") +>
      DelayFor("div#search img",50) !)
      .wgetJoin("div#search img",1,"src")
      .save("#{_}", "file:///home/peng/spookystuff/image")
      .collect().foreach(println(_))

    sc.stop()
  }
}
