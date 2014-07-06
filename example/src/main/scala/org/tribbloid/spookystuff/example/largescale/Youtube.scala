package org.tribbloid.spookystuff.example.largescale

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.example.SparkSubmittable

/**
 * Created by peng on 04/07/14.
 */
class Youtube extends SparkSubmittable{

  override def doMain() {
    (((sc.parallelize(Seq("MetallicaTV")) +>
      Visit("http://www.youtube.com/user/#{_}/videos") +>
      Loop() (
        Click("button.load-more-button span.load-more-text"),
        DelayFor("button.load-more-button span.hid.load-more-loading", 10)
      ) !).leftJoinBySlice("li.channels-content-item").selectInto(
        "title" -> (_.text1("h3.yt-lockup-title"))
      ).leftVisit(
        "h3.yt-lockup-title a.yt-uix-tile-link", limit = 1
      ) +>
      ExeScript("window.scrollBy(0,500)") +>
      DelayFor("iframe[title^=Comment]", 50) !><).selectInto(
        "description" -> (_.text1("div#watch-description-text")),
        "publish" -> (_.text1("p#watch-uploader-info")),
        "total_view" -> (_.text1("div#watch7-views-info span.watch-view-count")),
        "like_count" -> (_.text1("div#watch7-views-info span.likes-count")),
        "dislike_count" -> (_.text1("div#watch7-views-info span.dislikes-count"))
      ).leftVisit("iframe[title^=Comment]",limit=10,attr = "abs:src") +>
      Loop() (
        Click("span[title^=Load]"),
        DelayFor("span.PA[style^=display]",10)
      ) !).saveAs(dir="file:///home/peng/youtube").selectInto(
        "num_comments" -> (_.text1("div.DJa"))
      ).leftJoinBySlice(
        "div[id^=update]"
      ).map(
        page => (
          page.context.get("_"),
          page.context.get("title"),
          page.context.get("description"),
          page.context.get("publish"),
          page.context.get("total_view"),
          page.context.get("like_count"),
          page.context.get("dislike_count"),
          page.context.get("num_comments"),
          page.text1("h3.Mpa"),
          page.text1("div.Al")
          ).productIterator.toList.mkString("\t")
      ).saveAsTextFile("file:///home/peng/youtube/result")

//      .leftJoinBySlice()

//      .dump(dir="file:///home/peng/youtube"))




    //      Loop() (
    //        Click("button.load-more-button span.load-more-text"),
    //        DelayFor("button.load-more-button span.hid.load-more-loading", 5)
    //      )
    //
    //      .dump(dir="file:///home/peng/temp/youtube")

    //      .wgetJoin(
    //
    //      )
  }
}
