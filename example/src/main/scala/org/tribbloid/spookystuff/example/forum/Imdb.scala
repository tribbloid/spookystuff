package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore

/**
 * Created by peng on 20/08/14.
 */
object Imdb extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    noInput
      .fetch(
        Wget("http://www.imdb.com/chart")
      )
      .flatSelect($("div#boxoffice tbody tr"))(
        A"tr td.titleColumn".ownText.replaceAll("\"","").trim ~ 'rank,
        A"tr td.titleColumn a".text ~ 'name,
        A("tr td.titleColumn span").text ~ 'year,
        A("tr td.ratingColumn",0).text ~ 'box_weekend,
        A("td.ratingColumn",1).text ~ 'box_gross,
        A("tr td.weeksColumn").text ~ 'weeks
      )
      .wgetJoin($"tr td.titleColumn a") //go to movie pages, e.g. http://www.imdb.com/title/tt2015381/?ref_=cht_bo_1
      .select(
        $("td#overview-top div.titlePageSprite").text ~ 'score,
        $("td#overview-top span[itemprop=ratingCount]").text ~ 'rating_count,
        $("td#overview-top span[itemprop=reviewCount]").text ~ 'review_count
      )
      .wgetJoin($("div#maindetails_quicklinks a:contains(Reviews)")) //go to review pages, e.g. http://www.imdb.com/title/tt2015381/reviews?ref_=tt_urv
      .wgetExplore($"div#tn15content a:has(img[alt~=Next])", depthKey = 'page) //grab all pages by using the right arrow button.
      .flatSelect($("div#tn15content div:has(h2)"))(
        A("img[alt]").attr("alt") ~ 'review_rating,
        A("h2").text ~ 'review_title,
        A("small").text ~ 'review_meta
      )
      .wgetJoin($("a")) //go to reviewers' page, e.g. http://www.imdb.com/user/ur23582121/
      .select(
        $("div.user-profile h1").text ~ 'user_name,
        $("div.user-profile div.timestamp").text ~ 'user_timestamp,
        $("div.user-lists div.see-more").text ~ 'user_post_count,
        $("div.ratings div.see-more").text ~ 'user_rating_count,
        $("div.reviews div.see-more").text ~ 'user_review_count,
        $("div.overall div.histogram-horizontal a").attrs("title") ~ 'user_rating_histogram
      )
      .toSchemaRDD()
  }
}
