package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.entity.clientaction._

object Dealmoon extends SpookyTestCore {

  import spooky._

  def doMain() = {

    (sc.parallelize(Seq(null))
      +> Wget("http://www.dealmoon.com/Online-Stores/Amazon-com?expired=n")
      !=!()
      ).paginate("a.next_link")()
      .extract(
        "name" -> (_.text("div.mlist div.mtxt h2 span:not([style])"))
      )
      .asSchemaRDD()
  }
}
