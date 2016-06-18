package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.{Snapshot, Visit}
import com.tribbloids.spookystuff.session.DriverSession
import com.tribbloids.spookystuff.{SpookyEnvSuite, dsl}

class TestPageFromBrowser extends SpookyEnvSuite {

  import dsl._

  test("empty page") {
    val emptyPage: Doc = {
      val pb = new DriverSession(spooky)

      Snapshot(DocFilters.AllowStatusCode2XX).apply(pb).toList.head.asInstanceOf[Doc]
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
    val page = resultsList(0).asInstanceOf[Doc]

    page.autoSave(spooky, overwrite = true)

    val loadedContent = DocUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }
}