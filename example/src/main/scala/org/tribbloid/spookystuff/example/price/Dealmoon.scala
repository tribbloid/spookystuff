package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.QueryCore
import org.tribbloid.spookystuff.dsl._

object Dealmoon extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    spooky
      .fetch(
        Wget("http://www.dealmoon.com/Online-Stores/Amazon-com?expired=n")
      )
      .wgetExplore($"div.pagelink a", depthKey = 'page)
      .flatten(
        $"div.mlist div.mtxt h2 span:not([style])".text ~ 'name
      )
      .toDataFrame()
  }
}