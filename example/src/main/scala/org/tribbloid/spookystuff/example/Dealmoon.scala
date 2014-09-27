package org.tribbloid.spookystuff.example

import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.example.AppliancePartsPros._

object Dealmoon extends ExampleCore {

  def doMain() = {

    import spooky._

    (noInput
      +> Wget("http://www.dealmoon.com/Online-Stores/Amazon-com?expired=n")
      !=!()
      ).paginate("a.next_link")()
      .extract(
        "name" -> (_.text("div.mlist div.mtxt h2 span:not([style])"))
      )
      .asSchemaRDD()
  }
}