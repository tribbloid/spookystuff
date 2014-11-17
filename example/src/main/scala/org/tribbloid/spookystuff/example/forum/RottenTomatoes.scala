package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.example.ExampleCore

/**
* Created by peng on 20/08/14.
*/
object RottenTomatoes extends ExampleCore {
  override def doMain(spooky: SpookyContext) = {
    import spooky._

    noInput
    .fetch(
        Wget("http://www.rottentomatoes.com/")
      )
      .wgetJoin('* href "table.top_box_office tr.sidebarInTheaterTopBoxOffice a", indexKey = 'rank)() //go to movie page, e.g. http://www.rottentomatoes.com/m/guardians_of_the_galaxy/
      .extract(
        "name" -> (_.text1("h1.movie_title")),
        "meter" -> (_.text1("div#all-critics-numbers span#all-critics-meter")),
        "rating" -> (_.text1("div#all-critics-numbers p.critic_stats span")),
        "review_count" -> (_.text1("div#all-critics-numbers p.critic_stats span[itemprop=reviewCount]"))
      )
      .wgetJoin('* href "div#contentReviews h3 a")() //go to review page, e.g. http://www.rottentomatoes.com/m/guardians_of_the_galaxy/reviews/
      .paginate("div.scroller a.right")(indexKey = 'page) // grab all pages by using right arrow button
      .sliceJoin("div#reviews div.media_block")() //slice into review blocks
      .extract(
        "critic_name" -> (_.text1("div.criticinfo strong a")),
        "critic_org" -> (_.text1("div.criticinfo em.subtle")),
        "critic_review" -> (_.text1("div.reviewsnippet p")),
        "critic_score" -> (_.text1("div.reviewsnippet p.subtle",own = true))
      )
      .wgetJoin('* href "div.criticinfo strong a")() //go to critic page, e.g. http://www.rottentomatoes.com/critic/sean-means/
      .extract(
        "total_reviews_ratings" -> (_.text("div.media_block div.clearfix dd").toString)
      )
      .asSchemaRDD()
  }
}
