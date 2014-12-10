package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore
import org.tribbloid.spookystuff.dsl._

object Dealmoon extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    noInput
      .fetch(
        Wget("http://www.dealmoon.com/Online-Stores/Amazon-com?expired=n")
      )
      .wgetExplore($"div.pagelink a", depthKey = 'page)
      .flatten(
        $"div.mlist div.mtxt h2 span:not([style])".text > 'name
      )
      .asSchemaRDD()
  }
}