package org.tribbloid.spookystuff.example.largescale

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.example.SparkSubmittable

/**
 * Created by peng on 10/06/14.
 */
object GoogleImage extends SparkSubmittable {

  override def doMain() {

    ((sc.parallelize(Seq("dummy")) +>
      Visit("http://www.utexas.edu/world/univ/alpha/") !)
      .flatMap(_.text("div.box2 a", limit = Int.MaxValue, distinct = true))
      .repartition(400) +> //importantissimo! otherwise will only have 2 partitions
      Visit("http://images.google.com/") +>
      DelayFor("form[action=\"/search\"]",50) +>
      TextInput("input[name=\"q\"]","#{_} Logo") +>
      Submit("input[name=\"btnG\"]") +>
      DelayFor("div#search",50) !).wgetJoin(
        "div#search img",1,"src"
      ).dump("#{_}", "s3n://college-logo").foreach(println(_))
  }
}