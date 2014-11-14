package org.tribbloid.spookystuff.integration.forum

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 04/07/14.
 */
object Youtube extends TestCore{

  override def doMain() = {

    import spooky._

    import scala.concurrent.duration._

    val catalog = sc.parallelize(Seq("MetallicaTV"))
      .fetch(
        Visit("http://www.youtube.com/user/#{_}/videos")
          +> Loop(
          Click("button.load-more-button span.load-more-text")
            :: WaitFor("button.load-more-button span.hid.load-more-loading").in(10.seconds)
            :: Nil,
          1
        )
      )
      .sliceJoin("li.channels-content-item")()
      .extract("title" -> (_.text1("h3.yt-lockup-title")))
      .join('* href "h3.yt-lockup-title a.yt-uix-tile-link" as '~, limit = 1)(
        Visit("#{~}")
          +> ExeScript("window.scrollBy(0,500)")
          +> Try(WaitFor("iframe[title^=Comment]").in(50.seconds) :: Nil)
      )
      .repartition(400)
      .extract(
        "description" -> (_.text1("div#watch-description-text")),
        "publish" -> (_.text1("p#watch-uploader-info")),
        "total_view" -> (_.text1("div#watch7-views-info span.watch-view-count")),
        "like_count" -> (_.text1("div#watch7-views-info span.likes-count")),
        "dislike_count" -> (_.text1("div#watch7-views-info span.dislikes-count"))
      )
      .persist()

    println(catalog.count())

    val video = catalog
      .join('* src "iframe[title^=Comment]" as '~)(
        Visit("#{~}")
          +> Loop(
          Click("span[title^=Load]")
            :: WaitFor("span.PA[style^=display]").in(10.seconds)
            :: Nil
        )
      )
      .extract("num_comments" -> (_.text1("div.DJa")))
      .persist()

    println(video.count())

    val result = video
      .sliceJoin("div[id^=update]")()
      .extract(
        "comment1" -> (_.text1("h3.Mpa")),
        "comment2" -> (_.text1("div.Al"))
      ).persist()

    println(result.count())

    result.asSchemaRDD()
  }
}
