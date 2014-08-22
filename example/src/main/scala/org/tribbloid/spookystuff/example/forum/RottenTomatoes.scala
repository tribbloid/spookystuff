package org.tribbloid.spookystuff.example.forum

import com.fasterxml.jackson.databind.ObjectWriter
import org.tribbloid.spookystuff.Const
import org.tribbloid.spookystuff.entity.Wget
import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.example.{SparkRunnable, SparkSubmittable}
import scala.collection.JavaConversions._

/**
 * Created by peng on 20/08/14.
 */
object RottenTomatoes extends SparkRunnable {
  override def doMain(): Unit = {
    (sc.parallelize(Seq("Dummy")) +>
      Wget("http://www.rottentomatoes.com/") !==)
      .wgetJoin("table.top_box_office tr.sidebarInTheaterTopBoxOffice a", indexKey = "rank")
      .selectInto(
        "name" -> (_.text1("h1.movie_title")),
        "meter" -> (_.text1("div#all-critics-numbers span#all-critics-meter")),
        "rating" -> (_.text1("div#all-critics-numbers p.critic_stats span")),
        "review_count" -> (_.text1("div#all-critics-numbers p.critic_stats span[itemprop=reviewCount]"))
      )
      .wgetJoin("div#contentReviews h3 a")
      .wgetInsertPagination("div.scroller a.right", indexKey = "page")
      .joinBySlice("div#reviews div.media_block")
      .selectInto(
        "critic_name" -> (_.text1("div.criticinfo strong a")),
        "critic_org" -> (_.text1("div.criticinfo em.subtle")),
        "critic_review" -> (_.text1("div.reviewsnippet p")),
        "critic_score" -> (_.ownText1("div.reviewsnippet p.subtle"))
      )
      .wgetJoin("div.criticinfo strong a")
      .selectInto(
        "total_reviews_ratings" -> (_.text("div.media_block div.clearfix dd").toString())
      )
      .map(page => Const.objectMapper.writeValueAsString(page.context))
      .saveAsTextFile("file:///home/peng/spookystuff/rottentomatoes/result")
  }
}
