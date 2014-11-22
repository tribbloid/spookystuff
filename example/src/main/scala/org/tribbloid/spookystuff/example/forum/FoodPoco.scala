package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.example.ExampleCore

/**
 * Created by peng on 10/7/14.
 */
object FoodPoco extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    val base = noInput
      .fetch(
        Wget("http://cd.food.poco.cn/restaurant/res_search.php?sp_id=107001&reslocateID=101022001&reslocateID1=&reslocateID2=&seatxt=%BB%F0%B9%F8&%CC%E1%BD%BB=%CB%D1+%CB%F7")
      )
      .wgetExplore('* href "div.page a[title=下一页]")(depthKey = 'page)
      .sliceJoin("div.page_content")(indexKey = 'row)
      .extract(
        "name" -> (_.text1("h2 a")),
        "info"-> (_.text1("div.pa_text")),
        "avg_price_per_capita" -> (_.text1("div.page_aq")),
        "stars" -> (_.attr1("div.iconimg > img","src").replaceAll("images/","").replaceAll(".png","")),
        "ratings" -> (_.text("div.iconimg div.pop3 ul").mkString("|"))
      )
      .wgetJoin('*.href("div.text_link a"))().persist()

    val RDD1 = base
      .extract(
        "type" -> (_ => "comment"),
        "link" -> (_.url)
      )
      .sliceJoin("div#food_comment_list ul.text_con")(indexKey = 'commentRow)
      .extract(
        "comment" -> (_.text1("div#res_cmts_content")),
        "user_ratings" -> (_.attr("li.ph_text p img","alt").mkString("|")),
        "user" -> (_.text1("li.ph_tag p")),
        "useful" -> (_.text1("p.red"))
      )

    val RDD2 = base
      .extract(
        "type" -> (_ => "review")
      )
      .wgetJoin('*.href("div.main_wrap > div.more a"), limit = 1)()
      .extract(
        "link" -> (_.url)
      )
      .sliceJoin("li.text")(indexKey = 'commentRow)
      .extract(
        "title" -> (_.text1("div.title a")),
        "comment" -> (_.text1("p.lh18")),
        "user" -> (_.text1("p.lh20 a")),
        "stats" -> (_.text1("p.fl", own = true)),
        "date" -> (_.text1("p.fr"))
      )

    import sql._

    RDD1.union(RDD2).asSchemaRDD()
      .orderBy( 'page.asc, 'row.asc)
  }
}
