package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore
import org.tribbloid.spookystuff.dsl._

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
        Visit('_)
      )
      .wgetExplore($("div.page > a.next"), depthKey='page)
      .join($("ul.shop-list > li"), indexKey = 'row)(
        Visit(x"${A"p.title > a.shopname".href}/review_all")
      )(
        A("span.big-name").text ~ 'title,
        A("span > a").text ~ 'review_count
      )
      .wgetExplore($("div.Pages > a.NextPage"), depthKey='comment_page)
      .flatSelect($("div.comment-list > ul > li"), indexKey = 'comment_row)(
        A("span.item-rank-rst").attr("class") ~ 'rating,
        A("span.time").text ~ 'date,
        A("span.comm-per").text ~ 'average_price,
        A("pan.rst:nth-of-type(1)").text ~ 'taste,
        A("span.rst:nth-of-type(3)").text ~ 'service,
        A("div.comment-txt > div.J_brief-cont").text ~ 'comment,
        A("div.comment-txt > div.J_extra-cont").text ~ 'comment_extra
      )
      .toSchemaRDD().persist()
  }
}