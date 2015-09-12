package com.tribbloids.spookystuff.pages

import com.tribbloids.spookystuff.actions.{Snapshot, Visit}
import com.tribbloids.spookystuff.session.DriverSession
import com.tribbloids.spookystuff.{SpookyEnvSuite, dsl}

/**
 * Created by peng on 10/17/14.
 */
class TestPageFromBrowser extends SpookyEnvSuite {

  import dsl._

  test("empty page") {
    val emptyPage: Page = {
      val pb = new DriverSession(spooky)

      Snapshot(ExportFilters.PassAll).doExe(pb).toList.head.asInstanceOf[Page]
    }

    assert (emptyPage.findAll("div.dummy").attrs("href").isEmpty)
    assert (emptyPage.findAll("div.dummy").codes.isEmpty)
    assert (emptyPage.findAll("div.dummy").isEmpty)
  }

  test("visit, save and load") {

    val results = (
      Visit("http://en.wikipedia.org") ::
        Snapshot().as('T) :: Nil
      ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Page]

    page.autoSave(spooky, overwrite = true)

    val loadedContent = PageUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }
}