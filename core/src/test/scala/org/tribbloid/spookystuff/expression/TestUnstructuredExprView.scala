package org.tribbloid.spookystuff.expression

import org.tribbloid.spookystuff.SparkEnvSuite
import org.tribbloid.spookystuff.actions.{Trace, Wget}
import org.tribbloid.spookystuff.entity.PageRow

/**
 * Created by peng on 12/3/14.
 */
class TestUnstructuredExprView extends SparkEnvSuite {

  import org.tribbloid.spookystuff.dsl._

  val page = Trace(
    Wget("http://www.wikipedia.org/").>('page) :: Nil
  ).resolve(spooky)
  val row = PageRow(pages = page)
    .select($"title".head.>('abc) :: Nil)

  test("uri"){
    assert('*.uri.apply(row).get === "http://www.wikipedia.org/")
    assert('page.uri.apply(row).get === "http://www.wikipedia.org/")
    assert('abc.uri.apply(row).get === "http://www.wikipedia.org/")
  }
}
