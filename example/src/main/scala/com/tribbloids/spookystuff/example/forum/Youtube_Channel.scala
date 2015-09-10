package com.tribbloids.spookystuff.example.forum

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.example.QueryCore

/**
 * Created by peng on 04/07/14.
 */
object Youtube_Channel extends QueryCore{

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    import scala.concurrent.duration._

    sc.parallelize("SymphonyX".split(",").map(_.trim))
      .fetch(
        Wget("https://www.youtube.com/channels?q='{_}")
      )
      .join(S"span.qualified-channel-title-text".slice(0,2))(
        Visit(x"${A"a".href}/videos")
          +> Loop(
          Click("button.load-more-button span.load-more-text").in(5.seconds)
            :: WaitFor("button.load-more-button span.hid.load-more-loading").in(5.seconds)
            :: Nil,
          limit = 1
        )
      )(
        'A.text ~ 'channel,
        S"span[class*=subscriber]".text ~ 'subscribers
      )
      .join(S"li.channels-content-item")(
        Visit(A"h3.yt-lockup-title a.yt-uix-tile-link".href)
          +> ExeScript("window.scrollBy(0,500)")
          +> Loop(
          Click("div.load-comments").in(5.seconds)
            :: WaitFor("div.load-comments").in(5.seconds)
            :: Nil,
          limit = 1
        )
      )(
        A"h3.yt-lockup-title".text ~ 'title,
        A"ul.yt-lockup-meta-info li:nth-of-type(1)".text.replaceAll("[ *]views","") ~ 'views
      )
      .select(
        S"button.yt-uix-button.like-button-renderer-like-button-unclicked".text ~ 'likes,
        S"button.yt-uix-button.like-button-renderer-dislike-button-unclicked".text ~ 'dislikes
      )
      .toDF()
    //      .persist()
    //
    //    println(catalog.count())
    //
    //    val video = catalog
    //      .fetch(
    //        Visit(S"iframe[title^=Comment]".src, hasTitle = false)
    //          +> Loop(
    //          Click("span[title^=Load]")
    //            +> WaitFor("span.PA[style^=display]").in(10.seconds)
    //        )
    //      )
    //      .select(S"div.DJa".text ~ 'num_comments)
    //      .persist()
    //
    //    println(video.count())
    //
    //    val result = video
    //      .flatSelect(S"div[id^=update]")(
    //        A"h3.Mpa".text ~ 'comment1,
    //        A"div.Al".text ~ 'comment2
    //      ).persist()
    //
    //    println(result.count())
    //
    //    result.toDF()
  }
}
