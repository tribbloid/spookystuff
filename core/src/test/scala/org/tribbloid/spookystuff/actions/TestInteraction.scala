package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.pages.Page

import scala.concurrent.duration
import scala.concurrent.duration._
/**
 * Created by peng on 2/19/15.
 */
class TestInteraction extends SpookyEnvSuite {

  import org.tribbloid.spookystuff.dsl._

  test("sizzle selector should work") {

    val results = Trace(
      Visit("http://www.wikipedia.org/") ::
        WaitFor("a.link-box:contains(English)") ::
        Snapshot() :: Nil
    ).resolve(spooky)

    val markup = results(0).asInstanceOf[Page].code.get
    assert(markup.contains("<title>Wikipedia</title>"))
  }

  test("click should not double click") {
    val results = Trace(
      (Visit("https://ca.vwr.com/store/search?&pimId=582903")
        +> Paginate("a[title=Next]", delay = 0.second)).head.self
    ).resolve(spooky)

    assert(results.size == 5)
  }
}