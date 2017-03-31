package com.tribbloids.spookystuff.example.forum

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.example.QueryCore
import com.tribbloids.spookystuff.dsl._

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
      .wgetExplore(S"div#survey-header ul.pagination a:contains(next)", depthKey = 'page)
      .flatExtract(S"div.review")(
        A("div.rating strong").text ~ 'rating,
        A("div.date span").text ~ 'date,
        A("p.review-body").text ~ 'body
      )
      .toDF(sort=true)
  }
}