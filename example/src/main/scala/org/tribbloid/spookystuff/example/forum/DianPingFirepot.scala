package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore
import org.tribbloid.spookystuff.expressions._

/**
 * Created by peng on 10/16/14.
 */
object DianPingFirepot extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    sc.parallelize(Seq(
      "http://www.dianping.com/search/keyword/8/0_%E6%88%90%E9%83%BD%E4%BE%BF%E5%88%A9%E5%BA%97/x1y50#sortBar",
      "http://www.dianping.com/search/category/8/10/g110o8x1y50#sortBar",
      "http://www.dianping.com/search/category/8/10/g110o8x51y70#sortBar",
      "http://www.dianping.com/search/category/8/10/g110o8x71y999#sortBar"
    ))
      .fetch(
        Visit("#{_}")
      )
      .paginate("div.page > a.next", wget = false)(indexKey='page)
      .sliceJoin("ul.shop-list > li")(indexKey = 'row)
      .extract(
        "title" -> (_.text1("span.big-name")),
        "review_count" -> (_.text1("span > a"))
      )
      .join('*.href("p.title > a.shopname").as('~), limit = 1)(Visit("#{~}/review_all"))
      .paginate("div.Pages > a.NextPage", wget = false)(indexKey='comment_page)
      .sliceJoin("div.comment-list > ul > li")(indexKey = 'comment_row)
      .extract(
        "rating" -> (_.attr1("span.item-rank-rst","class")),
        "date" -> (_.text1("span.time")),
        "average_price" -> (_.text1("span.comm-per")),
        "taste" -> (_.text1("span.rst:nth-of-type(1)")),
        "service" -> (_.text1("span.rst:nth-of-type(3)")),
        "comment" -> (_.text1("div.comment-txt > div.J_brief-cont")),
        "commentExtra" ->(_.text1("div.comment-txt > div.J_extra-cont"))
      )
      .asSchemaRDD().persist()
  }
}