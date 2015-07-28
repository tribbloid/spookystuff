package org.tribbloid.spookystuff.pages

import org.tribbloid.spookystuff.actions.{Snapshot, Visit}
import org.tribbloid.spookystuff.session.DriverSession
import org.tribbloid.spookystuff.{SpookyEnvSuite, dsl}

/**
 * Created by peng on 10/17/14.
 */
class TestPageFromBrowser extends SpookyEnvSuite {

  import dsl._

  test("empty page") {
    val emptyPage: Page = {
      val pb = new DriverSession(spooky)

      Snapshot().doExe(pb).toList.head.asInstanceOf[Page]
    }

    assert (emptyPage.findAll("div.dummy").attrs("href").isEmpty)
    assert (emptyPage.findAll("div.dummy").codes.isEmpty)
    assert (emptyPage.findAll("div.dummy").isEmpty)
  }

  test("visit, save and load") {

    val results = (
      Visit("http://en.wikipedia.org") ::
        Snapshot().as('T) :: Nil
      ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Page]

    page.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }
}