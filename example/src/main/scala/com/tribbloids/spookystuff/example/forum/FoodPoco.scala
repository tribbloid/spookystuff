package com.tribbloids.spookystuff.example.forum

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore

/**
 * Created by peng on 10/7/14.
 */
object FoodPoco extends QueryCore {

  override def doMain(spooky: SpookyContext) = {

    val base = spooky
      .fetch(
        Wget("http://cd.food.poco.cn/restaurant/res_search.php?sp_id=107001&reslocateID=101022001&reslocateID1=&reslocateID2=&seatxt=%BB%F0%B9%F8&%CC%E1%BD%BB=%CB%D1+%CB%F7")
      )
      .wgetExplore(S"div.pag a[title=下一页]", depthKey = 'page)
      .flatSelect(S("div.page_content"))(
        A("h2 a").text ~ 'name,
        A("div.pa_text").text ~ 'info,
        A("div.page_aq").text ~ 'avg_price_per_capita,
        A("div.iconimg > img").text.replaceAll("images/","").replaceAll(".png","") ~ 'stars,
        A("div.iconimg div.pop3 ul").texts.mkString("|") ~ 'ratings
      )
      .wgetJoin(A("div.text_link a")).persist()

    val RDD1 = base
      .select(
        "comment" ~ 'type,
        S.uri ~ 'uri
      )
      .flatSelect(S("div#food_comment_list ul.text_con"), ordinalKey = 'commentRow)(
        A("div#res_cmts_content").text ~ 'comment,
        A("li.ph_text p img").attrs("alt").mkString("|") ~ 'user_ratings,
        A("li.ph_tag p").text ~ 'user,
        A("p.red").text ~ 'useful
      )

    val RDD2 = base
      .wgetJoin(S("div.main_wrap > div.more a"))
      .select(
        "review" ~ 'type,
        S.uri ~ 'uri
      )
      .flatSelect(S("li.text"), ordinalKey = 'commentRow)(
        A("div.title a").text ~ 'title,
        A("p.lh18").text ~ 'comment,
        A("p.lh20 a").text ~ 'user,
        A("p.fl").ownText ~ 'stats,
        A("p.fr").text ~ 'date
      )

    import sql.implicits._

    RDD1.union(RDD2).toDF().orderBy( 'page.asc, 'row.asc).rdd
  }
}
