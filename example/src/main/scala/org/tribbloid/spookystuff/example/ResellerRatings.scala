package org.tribbloid.spookystuff.example

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._

/**
 * Created by peng on 18/06/14.
 */
object ResellerRatings extends SparkSubmittable {

  def doMain() {

    (sc.parallelize(Seq("Hewlett_Packard")) +>
      Wget(
        "http://www.resellerratings.com/store/#{_}") !!!
      ).wgetInsertPagination(
        "div#survey-header ul.pagination a:contains(next)"
      ).joinBySlice("div.review").map{ page =>
      (page.text1("div.rating strong"),
        page.text1("div.date span"),
        page.text1("p.review-body")
        ).productIterator.toList.mkString("\t")
    }.saveAsTextFile("s3n://spookystuff/reseller-ratings/result")
  }
}
