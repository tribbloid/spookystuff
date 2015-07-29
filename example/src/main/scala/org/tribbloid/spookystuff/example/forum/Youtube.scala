package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore

/**
 * Created by peng on 04/07/14.
 */
object Youtube extends QueryCore{

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    import scala.concurrent.duration._

    val catalog = sc.parallelize(Seq("MetallicaTV"))
      .fetch(
        Visit("http://www.youtube.com/user/'{_}/videos")
          +> Loop(
          Click("button.load-more-button span.load-more-text")
            :: WaitFor("button.load-more-button span.hid.load-more-loading").in(10.seconds)
            :: Nil,
          1
        )
      )
      .join(S"li.channels-content-item")(
        Visit(A"h3.yt-lockup-title a.yt-uix-tile-link".href)
          +> ExeScript("window.scrollBy(0,500)")
          +> Try(WaitFor("iframe[title^=Comment]").in(50.seconds)),
        numPartitions = 400
      )(
        A"h3.yt-lockup-title".text ~ 'title
      )
      .select(
        S"div#watch-description-text".text ~ 'description,
        S"strong.watch-time-text".text ~ 'publish,
        S"div.watch-view-count".text ~ 'total_view,
        S"button#watch-like".text ~ 'like_count,
        S"button#watch-dislike".text ~ 'dislike_count
      )
      .persist()

    println(catalog.count())

    val video = catalog
      .fetch(
        Visit(S"iframe[title^=Comment]".src)
          +> Loop(
          Click("span[title^=Load]")
            +> WaitFor("span.PA[style^=display]").in(10.seconds)
        )
      )
      .select(S"div.DJa".text ~ 'num_comments)
      .persist()

    println(video.count())

    val result = video
      .flatSelect(S"div[id^=update]")(
        A"h3.Mpa".text ~ 'comment1,
        A"div.Al".text ~ 'comment2
      ).persist()

    println(result.count())

    result.toDF()
  }
}
