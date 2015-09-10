package com.tribbloids.spookystuff.example.forum

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore

/**
* Created by peng on 20/08/14.
*/
object RottenTomatoes extends QueryCore {
  override def doMain(spooky: SpookyContext) = {

    spooky
    .fetch(
        Wget("http://www.rottentomatoes.com/")
      )
      .wgetJoin(S"table.top_box_office tr.sidebarInTheaterTopBoxOffice a", ordinalKey = 'rank) //go to movie page, e.g. http://www.rottentomatoes.com/m/guardians_of_the_galaxy/
      .select(
        S"h1.movie_title".text ~ 'name,
        S"div#all-critics-numbers span#all-critics-meter".text ~ 'meter,
        S"div#all-critics-numbers p.critic_stats span".text ~ 'meter,
        S"div#all-critics-numbers p.critic_stats span[itemprop=reviewCount]" ~ 'review_count
      )
      .wgetJoin(S"div#contentReviews h3 a") //go to review page, e.g. http://www.rottentomatoes.com/m/guardians_of_the_galaxy/reviews/
      .wgetExplore(S"div.scroller a.right", depthKey = 'page) // grab all pages by using right arrow button
      .flatSelect(S"div#reviews div.media_block")(
        A"div.criticinfo strong a".text ~ 'critic_name,
        A"div.criticinfo em.subtle".text ~ 'critic_org,
        A"div.reviewsnippet p".text ~ 'critic_review,
        A"div.reviewsnippet p.subtle".ownText ~ 'critic_score
      )
      .wgetJoin(A"div.criticinfo strong a") //go to critic page, e.g. http://www.rottentomatoes.com/critic/sean-means/
      .select(
        S"div.media_block div.clearfix dd".text ~ 'total_reviews_ratings
      )
      .toDF()
  }
}