package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.integration.SpookyTestCore
import org.tribbloid.spookystuff.integration.forum.Imdb._
import org.tribbloid.spookystuff.entity._

/**
 * Created by peng on 18/06/14.
 */
object ResellerRatings extends SpookyTestCore {

  import spooky._

  def doMain() = {

    (sc.parallelize(Seq("Hewlett_Packard")) +>
      Wget( "http://www.resellerratings.com/store/#{_}") !=!())
      .paginate( "div#survey-header ul.pagination a:contains(next)")(indexKey = "page")
      .sliceJoin("div.review")()
      .select(
        "rating" -> (_.text1("div.rating strong")),
        "date" -> (_.text1("div.date span")),
        "body" -> (_.text1("p.review-body"))
      )
      .asSchemaRDD()
  }
}
