package org.tribbloid.spookystuff.integration.forum

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SchemaRDD
import org.tribbloid.spookystuff.entity.clientaction._
import org.tribbloid.spookystuff.factory.driver.{RandomProxyDriverFactory, TorDriverFactory}
import org.tribbloid.spookystuff.integration.{ProxyFeed, Proxies, SpookyTestCore}

/**
 * Created by peng on 8/28/14.
 */
object WeiboNoSession extends ProxyFeed {

  import spooky._

  def doMain() = {

//    proxies.foreach(println)

    spooky.driverFactory = RandomProxyDriverFactory(proxies: _*)

    val df = new SimpleDateFormat("yyyy-MM-dd-HH")

    val start = df.parse("2014-06-01-00").getTime
    val end = df.parse("2014-06-05-00").getTime

    val range = start.to(end, 3600*1000).map(time => df.format(new Date(time)))

    val RDD = (sc.parallelize(range)
      +> Visit("http://s.weibo.com/wb/%25E9%2594%25A4%25E5%25AD%2590%25E6%2589%258B%25E6%259C%25BA&xsort=time&timescope=custom:#{_}:#{_}&Refer=g")
      +> RandomDelay(40, 80)
      +> DelayFor("div.search_feed dl.feed_list",60)
      !=!())
      .extract("count" -> (_.text("div.search_feed dl.feed_list").size))
      .sliceJoin("div.search_feed dl.feed_list")(indexKey = "item")
      .extract(
        "text" -> (_.text1("p > em")),
        "author" -> (_.text1("dd.content p:nth-of-type(1) > a:nth-of-type(1)")),
        "date" -> (_.text1("p.info:nth-of-type(2) a.date")),
        "from" -> (_.text1("p.info:nth-of-type(2) > a[target]"))
      )
      .asSchemaRDD()

    RDD.persist()

    RDD.saveAsTextFile("s3n://spOOky/dump/weibo")

    RDD
  }
}