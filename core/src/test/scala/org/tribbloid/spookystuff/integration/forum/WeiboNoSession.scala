package org.tribbloid.spookystuff.integration.forum

import java.text.SimpleDateFormat
import java.util.Date

import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.integration.SpookyTestCore

/**
* Created by peng on 8/28/14.
*/
object WeiboNoSession extends SpookyTestCore {

  def doMain() = {

    import spooky._

import scala.concurrent.duration._

    val df = new SimpleDateFormat("yyyy-MM-dd-HH")

    val start = df.parse("2014-06-01-00").getTime
    val end = df.parse("2014-06-01-01").getTime

    val range = start.to(end, 3600*1000).map(time => df.format(new Date(time)))

    val RDD = ((sc.parallelize(range)
      +> Visit("http://s.weibo.com/wb/%25E9%2594%25A4%25E5%25AD%2590%25E6%2589%258B%25E6%259C%25BA&xsort=time&timescope=custom:#{_}:#{_}&Refer=g")
      +> RandomDelay(40.seconds, 80.seconds)
      +> DelayFor("div.search_feed dl.feed_list").in(60.seconds)
      !=!())
      .extract(
        "count" -> (_.text("div.search_feed dl.feed_list").size),
        "CAPCHAS" -> (_.text1("p.code_tit"))
      )
      .sliceJoin("div.search_feed dl.feed_list")(indexKey = "item")
      .extract(
        "text" -> (_.text1("p > em")),
        "forum" -> (page => "weibo"),
        "source" -> (_.text1("p.info:nth-of-type(2) > a[target]")),
        "URL" -> (_.resolvedUrl),
        "author" -> (_.text1("dd.content p:nth-of-type(1) > a:nth-of-type(1)")),
        "date" -> (_.text1("p.info:nth-of-type(2) a.date")),
        "thumb ups" -> (_.text1("p.info:nth-of-type(2) span a:nth-of-type(1)")),
        "retweet" -> (_.text1("p.info:nth-of-type(n+2) span a:nth-of-type(2)")),
        "reply" -> (_.text1("p.info:nth-of-type(n+2) a:nth-of-type(4)"))
      )
      .visit("dd.content p:nth-of-type(1) > a:nth-of-type(1)")()
      .+> (RandomDelay(40.seconds, 80.seconds))
      .+> (DelayForDocumentReady)
      !=!())
      .extract(
        "author.CAPCHAS2" -> (_.text1("p.code_tit")),
        "author.follow" -> (_.text1("li.S_line1 strong")),
        "author.fans" -> (_.text1("li.follower strong")),
        "author.tweets" -> (_.text1("li.W_no_border strong")),
        "author.gender" -> (_.attr1("div.tags em.W_ico12","title")),
        "author.tags" -> (_.text1("div.tags")),
        "author.level" -> (_.attr1("span.W_level_ico span.W_level_num","title")),
        "author.credit" -> (_.text1("div.pf_star_info p:nth-of-type(1)")),
        "author.interests" -> (_.text1("div.pf_star_info p:nth-of-type(2)"))
      )
      .asSchemaRDD()

    RDD.persist()

    RDD.saveAsTextFile("file://"+System.getProperty("user.home")+"/spOOky/dump/weibo")

    RDD
  }
}