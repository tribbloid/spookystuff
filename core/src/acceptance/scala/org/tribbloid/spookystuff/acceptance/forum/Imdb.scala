package org.tribbloid.spookystuff.acceptance.forum

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.acceptance.SparkTestCore
import org.tribbloid.spookystuff.entity._

/**
 * Created by peng on 20/08/14.
 */
object Imdb extends SparkTestCore {

  override def doMain(): Array[_] = {

    (sc.parallelize(Seq("Dummy")) +>
      Wget("http://www.imdb.com/chart") !==)
      .joinBySlice("div#boxoffice tbody tr") //slice into rows of top office table
      .selectInto(
        "rank" -> (_.ownText1("tr td.titleColumn").replaceAll("\"","").trim),
        "name" -> (_.text1("tr td.titleColumn a")),
        "year" -> (_.text1("tr td.titleColumn span")),
        "box_weekend" -> (_.text("tr td.ratingColumn")(0)),
        "box_gross" -> (_.text("td.ratingColumn")(1)),
        "weeks" -> (_.text1("tr td.weeksColumn"))
      )
      .wgetJoin("tr td.titleColumn a") //go to movie pages, e.g. http://www.imdb.com/title/tt2015381/?ref_=cht_bo_1
      .selectInto(
        "score" -> (_.text1("td#overview-top div.titlePageSprite")),
        "rating_count" -> (_.text1("td#overview-top span[itemprop=ratingCount]")),
        "review_count" -> (_.text1("td#overview-top span[itemprop=reviewCount]"))
      )
      .wgetLeftJoin("div#maindetails_quicklinks a:contains(Reviews)") //go to review pages, e.g. http://www.imdb.com/title/tt2015381/reviews?ref_=tt_urv
      .wgetInsertPagination("div#tn15content a:has(img[alt~=Next])",500) //grab all pages by using the right arrow button.
      .joinBySlice("div#tn15content div:has(h2)") //slice into rows of reviews
      .selectInto(
        "review_rating" -> (_.attr1("img[alt]","alt")),
        "review_title" -> (_.text1("h2")),
        "review_meta" -> (_.text("small").toString())
      )
      .wgetLeftJoin("a") //go to reviewers' page, e.g. http://www.imdb.com/user/ur23582121/
      .selectInto(
        "user_name" -> (_.text1("div.user-profile h1")),
        "user_timestamp" -> (_.text1("div.user-profile div.timestamp")),
        "user_post_count" -> (_.ownText1("div.user-lists div.see-more")),
        "user_rating_count" -> (_.text1("div.ratings div.see-more")),
        "user_review_count" -> (_.text1("div.reviews div.see-more")),
        "user_rating_histogram" -> (_.attr("div.overall div.histogram-horizontal a","title").toString())
      )
      .asTsvRDD() //Output as TSV file
      .collect()
  }
}
