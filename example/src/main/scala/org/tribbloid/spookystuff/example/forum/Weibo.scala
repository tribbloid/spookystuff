package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.{dsl, SpookyContext}
import org.tribbloid.spookystuff.actions.{Loop, _}
import org.tribbloid.spookystuff.example.ExampleCore
import dsl._

import scala.concurrent.duration._

/**
 * Created by peng on 8/28/14.
 */
object Weibo extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    sc.parallelize(Seq("锤子手机"))
    .fetch(
        Visit("http://www.weibo.com/login.php")
          +> TextInput("div.username input.W_input","peng@anchorbot.com")
          +> TextInput("div.password input.W_input","A9e7k1")
          +> Click("div.info_list a.W_btn_g span").in(40.seconds)
          +> Delay(10.seconds)
          +> TextInput("input.gn_input", "'{_}\n")
          +> Click("ul.formbox_tab a:nth-of-type(2)")
          +> Delay(10.seconds)
          +> Snapshot()
          +> Loop(
          Click("ul.search_page_M li:last-of-type a") ::
            Delay(10.seconds) ::
            WaitFor("ul.search_page_M li:nth-of-type(10) a").in(10.seconds) ::
            Snapshot() :: Nil,
          50
        ),
        flattenPagesIndexKey = 'page
      )
      .flatSelect($"dl.feed_list", indexKey = 'item)(
        A"dl.feed_list p em".text > 'text
      )
      .asSchemaRDD()
  }
}