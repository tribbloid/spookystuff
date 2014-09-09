package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.factory.driver.TorDriverFactory
import org.tribbloid.spookystuff.integration.SpookyTestCore

/**
 * Created by peng on 8/28/14.
 */
object Weibo extends SpookyTestCore {

  import spooky._

  def doMain() = {

    spooky.driverFactory = TorDriverFactory()

    (sc.parallelize(Seq("锤子手机"))
      +> Visit("http://www.weibo.com/login.php")
      +> TextInput("div.username input.W_input","peng@anchorbot.com")
      +> TextInput("div.password input.W_input","A9e7k1")
      +> Click("div.info_list a.W_btn_g span",40)
      +> Delay(10)
      +> TextInput("input.gn_input", "#{_}\n")
      +> Click("ul.formbox_tab a:nth-of-type(2)")
      +> Delay(10)
      +> Snapshot()
      +> Loop(50) (
        Click("ul.search_page_M li:last-of-type a"),
        Delay(10),
        DelayFor("ul.search_page_M li:nth-of-type(10) a", 10),
        Snapshot()
      )
      !=!(indexKey = "page"))
      .sliceJoin("dl.feed_list")(indexKey = "item")
      .select(
        "text" -> (_.text1("dl.feed_list p em"))
      )
      .asSchemaRDD()
  }
}