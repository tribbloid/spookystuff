package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore
import org.tribbloid.spookystuff.expressions._

/**
 * Created by peng on 18/06/14.
 */
object ResellerRatings extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    sc.parallelize(Seq("Hewlett_Packard"))
      .fetch(
        Wget( "http://www.resellerratings.com/store/#{_}")
      )
      .wgetExplore('* href "div#survey-header ul.pagination a:contains(next)")(depthKey = 'page)
      .sliceJoin("div.review")()
      .extract(
        "rating" -> (_.text1("div.rating strong")),
        "date" -> (_.text1("div.date span")),
        "body" -> (_.text1("p.review-body"))
      )
      .asSchemaRDD()
  }
}
