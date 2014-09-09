package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.factory.driver.TorDriverFactory
import org.tribbloid.spookystuff.integration.SpookyTestCore

/**
 * Created by peng on 8/28/14.
 */
object WeiboNoSession extends SpookyTestCore {

  import spooky._

  def doMain() = {

//    spooky.driverFactory = TorDriverFactory

    (sc.parallelize(Seq("锤子手机"))
      +> Visit("http://s.weibo.com/wb/smartisan&xsort=time&timescope=custom:2014-05-01-12:2014-05-04-13&Refer=g")
      !=!())
      .sliceJoin("dl.feed_list")(indexKey = "item")
      .select(
        "text" -> (_.text1("p > em")),
        "author" -> (_.text1("dd.content p:nth-of-type(1) > a:nth-of-type(1)")),
        "date" -> (_.text1("p.info:nth-of-type(2) a.date")),
        "from" -> (_.text1("p.info:nth-of-type(2) > a[target]"))
      )
      .asSchemaRDD()
  }
}