package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
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
      .wgetJoin($"table.top_box_office tr.sidebarInTheaterTopBoxOffice a", indexKey = 'rank)() //go to movie page, e.g. http://www.rottentomatoes.com/m/guardians_of_the_galaxy/
      .select(
        $"h1.movie_title".text > 'name,
        $"div#all-critics-numbers span#all-critics-meter".text > 'meter,
        $"div#all-critics-numbers p.critic_stats span".text > 'meter,
        $"div#all-critics-numbers p.critic_stats span[itemprop=reviewCount]" > 'review_count
      )
      .wgetJoin($"div#contentReviews h3 a")() //go to review page, e.g. http://www.rottentomatoes.com/m/guardians_of_the_galaxy/reviews/
      .wgetExplore($"div.scroller a.right")(depthKey = 'page) // grab all pages by using right arrow button
      .flatSelect($"div#reviews div.media_block")(
        A"div.criticinfo strong a".text > 'critic_name,
        A"div.criticinfo em.subtle".text > 'critic_org,
        A"div.reviewsnippet p".text > 'critic_review,
        A"div.reviewsnippet p.subtle".ownText > 'critic_score
      ) //slice into review blocks
      .wgetJoin(A"div.criticinfo strong a")() //go to critic page, e.g. http://www.rottentomatoes.com/critic/sean-means/
      .select(
        $"div.media_block div.clearfix dd".text > 'total_reviews_ratings
      )
      .asSchemaRDD()
  }
}
