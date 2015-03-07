package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.pages.Page

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
}