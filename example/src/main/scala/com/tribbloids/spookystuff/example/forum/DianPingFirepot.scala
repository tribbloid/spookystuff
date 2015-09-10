package com.tribbloids.spookystuff.example.forum

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.example.QueryCore
import com.tribbloids.spookystuff.dsl._

/**
 * Created by peng on 10/16/14.
 */
object DianPingFirepot extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    sc.parallelize(Seq(
      "http://www.dianping.com/search/keyword/8/0_%E6%88%90%E9%83%BD%E4%BE%BF%E5%88%A9%E5%BA%97/x1y50#sortBar",
      "http://www.dianping.com/search/category/8/10/g110o8x1y50#sortBar",
      "http://www.dianping.com/search/category/8/10/g110o8x51y70#sortBar",
      "http://www.dianping.com/search/category/8/10/g110o8x71y999#sortBar"
    ))
      .fetch(
        Visit('_)
      )
      .wgetExplore(S("div.page > a.next"), depthKey='page)
      .join(S("ul.shop-list > li"), ordinalKey = 'row)(
        Visit(x"${A"p.title > a.shopname".href}/review_all")
      )(
        A("span.big-name").text ~ 'title,
        A("span > a").text ~ 'review_count
      )
      .wgetExplore(S("div.Pages > a.NextPage"), depthKey='comment_page)
      .flatSelect(S("div.comment-list > ul > li"), ordinalKey = 'comment_row)(
        A("span.item-rank-rst").attr("class") ~ 'rating,
        A("span.time").text ~ 'date,
        A("span.comm-per").text ~ 'average_price,
        A("pan.rst:nth-of-type(1)").text ~ 'taste,
        A("span.rst:nth-of-type(3)").text ~ 'service,
        A("div.comment-txt > div.J_brief-cont").text ~ 'comment,
        A("div.comment-txt > div.J_extra-cont").text ~ 'comment_extra
      )
      .toDF()
  }
}