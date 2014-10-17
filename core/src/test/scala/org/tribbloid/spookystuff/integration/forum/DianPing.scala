package org.tribbloid.spookystuff.integration.forum

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 10/16/14.
 */
object DianPing extends TestCore {

  import spooky._

  override def doMain(): SchemaRDD = {

    (sc.parallelize(Seq(
      "http://www.dianping.com/search/category/8/10/g110o8x1y50#sortBar",
      "http://www.dianping.com/search/category/8/10/g110o8x51y70#sortBar",
      "http://www.dianping.com/search/category/8/10/g110o8x71y999#sortBar"
    ),3)
      +> Wget("#{_}")
      !=!())
      .paginate("div.page > a.next")(indexKey="page")
      .sliceJoin("ul.shop-list > li")(indexKey = "row")
      .extract(
        "title" -> (_.text1("span.big-name")),
        "review_count" -> (_.text1("span > a"))
      )
      .dropActions()
      .+*%>(
        Wget("#{~}/review_all") -> (_.attr("p.title > a.shopname", "abs:href"))
      )(limit =1)
      .!><()
      .paginate("div.Pages > a.NextPage")(indexKey="page")
      .sliceJoin("div.comment-list > ul > li")(indexKey = "row")
      .extract(
        "rating" -> (_.attr1("span.item-rank-rst","class")),
        "date" -> (_.text1("span.time")),
        "average_price" -> (_.text1("span.comm-per")),
        "taste" -> (_.text1("span.rst:nth-of-type(1)")),
        "service" -> (_.text1("span.rst:nth-of-type(3)")),
        "comment" -> (_.text1("div.comment-txt > div.J_brief-cont")),
        "commentExtra" ->(_.text1("div.comment-txt > div.J_extra-cont"))
      )
      .asSchemaRDD()
  }
}
