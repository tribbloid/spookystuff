package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.entity.clientaction._
import org.tribbloid.spookystuff.entity.clientaction.Loop
import org.tribbloid.spookystuff.integration.SpookyTestCore
import org.tribbloid.spookystuff.operator.LeftOuter

/**
* Created by peng on 04/07/14.
*/
object Youtube extends SpookyTestCore{

  override def doMain() = {

    import spooky._
    
    (((sc.parallelize(Seq("MetallicaTV")) +>
      Visit("http://www.youtube.com/user/#{_}/videos") +>
      Loop(2) (
        Click("button.load-more-button span.load-more-text"),
        DelayFor("button.load-more-button span.hid.load-more-loading", 10)
      ) !=!())
      .sliceJoin("li.channels-content-item")(limit=100)
      .extract("title" -> (_.text1("h3.yt-lockup-title")))
      .visit("h3.yt-lockup-title a.yt-uix-tile-link")(limit = 1)
      .repartition(400) +>
      ExeScript("window.scrollBy(0,500)") +>
      DelayFor("iframe[title^=Comment]", 50).canFail()
      !><()).extract(
        "description" -> (_.text1("div#watch-description-text")),
        "publish" -> (_.text1("p#watch-uploader-info")),
        "total_view" -> (_.text1("div#watch7-views-info span.watch-view-count")),
        "like_count" -> (_.text1("div#watch7-views-info span.likes-count")),
        "dislike_count" -> (_.text1("div#watch7-views-info span.dislikes-count"))
      )
      .visit("iframe[title^=Comment]", attr = "abs:src")(limit = 10) +>
      Loop(2) (
        Click("span[title^=Load]"),
        DelayFor("span.PA[style^=display]",10)
      ) !=!(joinType = LeftOuter))
      .extract("num_comments" -> (_.text1("div.DJa")))
      .sliceJoin("div[id^=update]")()
      .extract(
        "comment1" -> (_.text1("h3.Mpa")),
        "comment2" -> (_.text1("div.Al"))
      )
      .asSchemaRDD()
  }
}
