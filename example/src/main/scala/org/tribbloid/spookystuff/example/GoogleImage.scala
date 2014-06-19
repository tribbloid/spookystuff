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
    val conf = new SparkConf().setAppName("MoreLinkedIn")
    conf.setMaster("local[8,3]")
    //    conf.setMaster("local-cluster[2,4,1000]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val jars = SparkContext.jarOfClass(this.getClass).toList
    conf.setJars(jars)
    conf.set("spark.task.maxFailures", "3")
    val sc = new SparkContext(conf)

    Conf.init(sc)

//    val nameRDD = sc.textFile("/home/peng/Documents/affiliation.txt").distinct(16)
    val nameRDD = sc.textFile("/home/peng/Documents/affiliation_short5.txt",16)

    val actionsRDD = nameRDD +>
      Visit("http://images.google.com/") +>
      DelayFor("form[action=\"/search\"]",50) +>
      TextInput("input[name=\"q\"]","#{_} Logo") +>
      Submit("input[name=\"btnG\"]") +>
      DelayFor("div#search img",50) +>
      Snapshot()

    val pageRDD = (actionsRDD !!!).selectInto(
    "imgSrc" -> {_.attrFirst("div#search img","src")}
    )

//    pageRDD.map(page => page.context).collect().foreach(println(_))

    val imgRDD = (pageRDD +>
    Wget("#{imgSrc}")) !!!

//    imgRDD.map(page => page.context).collect().foreach(println(_))

    val saveRDD = imgRDD.map(page => page.save("#{_}", "file:///home/peng/spookystuff/image"))
////
    saveRDD.collect().foreach(println(_))

    sc.stop()
  }
}
