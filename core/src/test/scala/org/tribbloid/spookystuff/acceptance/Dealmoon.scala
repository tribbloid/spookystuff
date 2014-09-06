package org.tribbloid.spookystuff.acceptance

import org.tribbloid.spookystuff.entity._

object Dealmoon extends SpookyTestCore {

  import spooky._

  def doMain() = {

    (sc.parallelize(Seq(null))
      +> Wget("http://www.dealmoon.com/Online-Stores/Amazon-com?expired=n")
      !=!()
      ).paginate("a.next_link")()
      .select(
        "name" -> (_.text("div.mlist div.mtxt h2 span:not([style])"))
      )
      .asSchemaRDD()
  }
}
