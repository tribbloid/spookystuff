package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.example.SparkSubmittable

/**
 * Created by peng on 18/06/14.
 */
object ResellerRatings extends SparkSubmittable {

  def doMain() {

    (sc.parallelize(Seq("Hewlett_Packard")) +>
      Wget(
        "http://www.resellerratings.com/store/#{_}") !==
      ).wgetInsertPagination(
        "div#survey-header ul.pagination a:contains(next)"
      ).joinBySlice("div.review").map{ page =>
      (page.context.get("_"), //just check if it is preserved
        page.text1("div.rating strong"),
        page.text1("div.date span"),
        page.text1("p.review-body")
        ).productIterator.toList.mkString("\t")
    }.collect.foreach(println(_))
  }
}
