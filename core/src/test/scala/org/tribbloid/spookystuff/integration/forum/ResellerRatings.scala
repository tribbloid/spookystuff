package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 18/06/14.
 */
object ResellerRatings extends TestCore {

  import spooky._

  def doMain() = {

    (sc.parallelize(Seq("Hewlett_Packard")) +>
      Wget( "http://www.resellerratings.com/store/#{_}") !=!())
      .paginate( "div#survey-header ul.pagination a:contains(next)")(indexKey = "page")
      .sliceJoin("div.review")()
      .extract(
        "rating" -> (_.text1("div.rating strong")),
        "date" -> (_.text1("div.date span")),
        "body" -> (_.text1("p.review-body"))
      )
      .asSchemaRDD()
  }
}
