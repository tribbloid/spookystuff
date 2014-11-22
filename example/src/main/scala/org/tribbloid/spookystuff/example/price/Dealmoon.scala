package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore
import org.tribbloid.spookystuff.expressions._

object Dealmoon extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    noInput
      .fetch(
        Wget("http://www.dealmoon.com/Online-Stores/Amazon-com?expired=n")
      )
      .wgetExplore('* href "div.pagelink a")(depthKey = 'page)
      .extract(
        "name" -> (_.text("div.mlist div.mtxt h2 span:not([style])"))
      )
      .flatten('name)
      .asSchemaRDD()
  }
}