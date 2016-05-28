package com.tribbloids.spookystuff.example.forum

import java.text.SimpleDateFormat
import java.util.Date

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore
import com.tribbloids.spookystuff.extractors.Extractor

/**
 * Created by peng on 8/28/14.
 */
object WeiboNoSession extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    import scala.concurrent.duration._

    val df = new SimpleDateFormat("yyyy-MM-dd-HH")

    val start = df.parse("2014-06-14-09").getTime
    val end = df.parse("2014-06-14-10").getTime

    val range = start.until(end, 3600*1000).map(time => df.format(new Date(time)))

    sc.parallelize(range,1000)
      .fetch(
        RandomDelay(40.seconds, 80.seconds)
          +> Visit("http://s.weibo.com/wb/%25E6%2588%2590%25E9%2583%25BD%25E9%2593%25B6%25E8%25A1%258C&xsort=time&timescope=custom:'{_}:'{_}&Refer=g")
          +> Try(WaitFor("div.search_feed dl.feed_list").in(60.seconds))
      )
      .select(
        A"div.search_feed dl.feed_list".size ~ 'count,
        A"p.code_tit".text ~ 'CAPCHAS
      )
      .flatExtract(S"div.search_feed dl.feed_list", ordinalKey = 'item)(
        "成都银行" ~ 'name,
        A"p > em".text ~ 'text,
        "weibo" ~ 'forum,
        A"p.info:nth-of-type(2) > a[target]".text ~ 'source,
        'A.uri ~ 'URI,
        A"dd.content p:nth-of-type(1) > a:nth-of-type(1)".text ~ 'author,
        A"p.info:nth-of-type(2) a.date".text ~ 'date,
        A"p.info:nth-of-type(2) span a:nth-of-type(1)".text ~ 'thumb_ups,
        A"p.info:nth-of-type(n+2) span a:nth-of-type(2)".text ~ 'retweet,
        A"p.info:nth-of-type(n+2) a:nth-of-type(4)".text ~ 'reply
      )
      .join(A"dd.content p:nth-of-type(1) > a:nth-of-type(1)")(
        Visit('A.href)
          .+> (RandomDelay(40.seconds, 80.seconds))
          .+> (WaitForDocumentReady)
      )()
      .select(
        S"p.code_tit".text ~ 'author_CAPCHAS,
        S"li.S_line1 strong".text ~ 'author_follow,
        S"li.follower strong".text ~ 'author_fans,
        S"li.W_no_border strong".text ~ 'author_tweets,
        S"div.tags em.W_ico12".attr("title") ~ 'author_gender,
        S"div.tags".text ~ 'author_tags,
        S"span.W_level_ico span.W_level_num".attr("title") ~ 'author_level,
        S"div.pf_star_info p:nth-of-type(1)".text ~ 'author_credit,
        S"div.pf_star_info p:nth-of-type(2)".text ~ 'author_interests
      )
      .toDF()
  }
}