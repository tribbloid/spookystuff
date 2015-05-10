package org.tribbloid.spookystuff.expression

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.actions.{Trace, Wget}
import org.tribbloid.spookystuff.entity.PageRow

/**
 * Created by peng on 12/3/14.
 */
class TestUnstructuredExprView extends SpookyEnvSuite {

  import org.tribbloid.spookystuff.dsl._

  lazy val page = (
    Wget("http://www.wikipedia.org/").~('page) :: Nil
  ).resolve(spooky).toArray
  lazy val row = PageRow(pageLikes = page)
    .select($"title".head.~('abc))
    .head

  test("uri"){
    assert($.uri.apply(row).get === "http://www.wikipedia.org/")
    assert('page.uri.apply(row).get === "http://www.wikipedia.org/")
    assert('abc.uri.apply(row).get === "http://www.wikipedia.org/")
  }
}