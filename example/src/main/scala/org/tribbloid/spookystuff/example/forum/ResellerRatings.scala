package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.QueryCore
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 18/06/14.
 */
object ResellerRatings extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    sc.parallelize(Seq("Hewlett_Packard"))
      .fetch(
        Wget( "http://www.resellerratings.com/store/'{_}")
      )
      .wgetExplore($"div#survey-header ul.pagination a:contains(next)", depthKey = 'page)
      .flatSelect($"div.review")(
        A("div.rating strong").text ~ 'rating,
        A("div.date span").text ~ 'date,
        A("p.review-body").text ~ 'body
      )
      .toDataFrame()
  }
}