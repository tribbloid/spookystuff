package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.integration.SpookyTestCore
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.operator.LeftOuter

/**
 * Created by peng on 20/08/14.
 */
object Imdb extends SpookyTestCore {

  override def doMain() = {

    import spooky._

    (sc.parallelize(Seq(null))
      +>  Wget("http://www.imdb.com/chart") !=!())
      .sliceJoin("div#boxoffice tbody tr")() //slice into rows of top office table
      .select(
        "rank" -> (_.text1("tr td.titleColumn", own = true).replaceAll("\"","").trim),
        "name" -> (_.text1("tr td.titleColumn a")),
        "year" -> (_.text1("tr td.titleColumn span")),
        "box_weekend" -> (_.text("tr td.ratingColumn")(0)),
        "box_gross" -> (_.text("td.ratingColumn")(1)),
        "weeks" -> (_.text1("tr td.weeksColumn"))
      )
      .wgetJoin("tr td.titleColumn a")() //go to movie pages, e.g. http://www.imdb.com/title/tt2015381/?ref_=cht_bo_1
      .select(
        "score" -> (_.text1("td#overview-top div.titlePageSprite")),
        "rating_count" -> (_.text1("td#overview-top span[itemprop=ratingCount]")),
        "review_count" -> (_.text1("td#overview-top span[itemprop=reviewCount]"))
      )
      .wgetJoin("div#maindetails_quicklinks a:contains(Reviews)")(joinType = LeftOuter) //go to review pages, e.g. http://www.imdb.com/title/tt2015381/reviews?ref_=tt_urv
      .paginate("div#tn15content a:has(img[alt~=Next])")(limit = 2) //grab all pages by using the right arrow button.
      .sliceJoin("div#tn15content div:has(h2)")(joinType = LeftOuter) //slice into rows of reviews
      .select(
        "review_rating" -> (_.attr1("img[alt]","alt")),
        "review_title" -> (_.text1("h2")),
        "review_meta" -> (_.text("small"))
      )

      .wgetJoin("a")(joinType = LeftOuter) //go to reviewers' page, e.g. http://www.imdb.com/user/ur23582121/
      .select(
        "user_name" -> (_.text1("div.user-profile h1")),
        "user_timestamp" -> (_.text1("div.user-profile div.timestamp")),
        "user_post_count" -> (_.text1("div.user-lists div.see-more", own = true)),
        "user_rating_count" -> (_.text1("div.ratings div.see-more")),
        "user_review_count" -> (_.text1("div.reviews div.see-more")),
        "user_rating_histogram" -> (_.attr("div.overall div.histogram-horizontal a","title"))
      )
      .asSchemaRDD()
  }
}
