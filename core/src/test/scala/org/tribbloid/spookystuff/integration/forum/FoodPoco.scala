package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.entity.client
import org.tribbloid.spookystuff.integration.TestCore
import org.tribbloid.spookystuff.entity.client._

/**
 * Created by peng on 10/7/14.
 */
object FoodPoco extends TestCore {

  import spooky._

  def doMain() = {

    (noInput
      +> Wget("http://cd.food.poco.cn/restaurant/res_search.php?sp_id=107001&reslocateID=101022001&reslocateID1=&reslocateID2=&seatxt=%BB%F0%B9%F8&%CC%E1%BD%BB=%CB%D1+%CB%F7")
      !=!())
      .paginate("div.page a[title=下一页]")(indexKey = "page", limit = 2)
      .sliceJoin("div.page_content")(indexKey = "row")
      .extract(
        "name" -> (_.text1("h2 a")),
        "info"-> (_.text1("div.pa_text")),
        "avg_price_per_capita" -> (_.text1("div.page_aq")),
        "stars" -> (_.attr1("div.iconimg > img","src").replaceAll("images/","").replaceAll(".png","")),
        "ratings" -> (_.text("div.iconimg div.pop3 ul").mkString("|"))
      )
      .wgetJoin("div.text_link a")(limit = 1)
      .sliceJoin("div#food_comment_list ul.text_con")(indexKey = "commentRow")
      .extract(
        "comment" -> (_.text1("div#res_cmts_content")),
        "user_ratings" -> (_.attr("li.ph_text p img","alt").mkString("|")),
        "user" -> (_.text1("li.ph_tag p")),
        "useful" -> (_.text1("p.red"))
      )
      .asSchemaRDD()
  }
}
