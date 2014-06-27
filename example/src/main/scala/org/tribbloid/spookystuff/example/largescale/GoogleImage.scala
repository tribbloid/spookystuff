package org.tribbloid.spookystuff.example.largescale

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.Conf
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.example.SparkSubmittable

/**
 * Created by peng on 10/06/14.
 */
object GoogleImage extends SparkSubmittable {

  override def doMain() {

    //    val nameRDD = sc.textFile("/home/peng/Documents/affiliation.txt").distinct(16)
    ((sc.parallelize(Seq("dummy")) +>
      Visit("http://www.utexas.edu/world/univ/alpha/") !)
      .flatMap(page => page.text("div.box2 a", limit = Int.MaxValue, distinct = true)) +>
      //      .flatMap(_.text("div.box2 a", limit = 10, distinct = true)) +>
      Visit("http://images.google.com/") +>
      DelayFor("form[action=\"/search\"]",50) +>
      TextInput("input[name=\"q\"]","#{_} Logo") +>
      Submit("input[name=\"btnG\"]") +>
      DelayFor("div#search img",50) !)
      .wgetJoin("div#search img",1,"src")
      .save("#{_}", "s3n://college-logo")
      .foreach(println(_))
  }
}
