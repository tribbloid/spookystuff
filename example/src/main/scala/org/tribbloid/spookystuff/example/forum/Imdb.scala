package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.example.{SparkRunnable, SparkSubmittable}
import scala.collection.JavaConversions._

/**
 * Created by peng on 20/08/14.
 */
object Imdb extends SparkRunnable {

  override def doMain(): Unit = {

    (sc.parallelize(Seq("Dummy")) +>
      Wget("http://www.imdb.com/chart") !)
      .joinBySlice("div#boxoffice tbody tr")
      .select(
        "rank" -> (_.ownText1("tr td.titleColumn").replaceAll("\"","").trim),
        "name" -> (_.text1("tr td.titleColumn a")),
        "year" -> (_.text1("tr td.titleColumn span")),
        "box_weekend" -> (_.text("tr td.ratingColumn")(0)),
        "box_gross" -> (_.text("td.ratingColumn")(1)),
        "weeks" -> (_.text1("tr td.weeksColumn"))
      )
      .wgetJoin("tr td.titleColumn a")
      .selectInto(
        "score" -> (_.text1("td#overview-top div.titlePageSprite")),
        "rating_count" -> (_.text1("td#overview-top span[itemprop=ratingCount]")),
        "review_count" -> (_.text1("td#overview-top span[itemprop=reviewCount]"))
      )
      .wgetLeftJoin("div#maindetails_quicklinks a:contains(Reviews)")
      .wgetInsertPagination("div#tn15content a:has(img[alt~=Next])",500)
      .saveAs(dir = "file:///home/peng/spookystuff/imdb")
      .joinBySlice("div#tn15content div:has(h2)")
      .selectInto(
        "review_rating" -> (_.attr1("img[alt]","alt")),
        "review_title" -> (_.text1("h2")),
        "review_meta" -> (_.text("small").toString())
      )
      .wgetLeftJoin("a")
      .selectInto(
        "user_name" -> (_.text1("div.user-profile h1")),
        "user_timestamp" -> (_.text1("div.user-profile div.timestamp")),
        "user_post_count" -> (_.ownText1("div.user-lists div.see-more")),
        "user_rating_count" -> (_.text1("div.ratings div.see-more")),
        "user_review_count" -> (_.text1("div.reviews div.see-more")),
        "user_rating_histogram" -> (_.attr("div.overall div.histogram-horizontal a","title").toString())
      )

      .map(_.context.values().mkString("\t"))
      .saveAsTextFile("file:///home/peng/spookystuff/imdb/result")

//      .wgetInsertPagination(
//        "div#survey-header ul.pagination a:contains(next)"
//      ).joinBySlice("div.review").map{ page =>
//      (page.context.get("_"), //just check if it is preserved
//        page.text1("div.rating strong"),
//        page.text1("div.date span"),
//        page.text1("p.review-body")
//        ).productIterator.toList.mkString("\t")
//    }.collect.foreach(println(_))
  }
}
