package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.{Snapshot, Visit}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.{SpookyEnvFixture, dsl}

class TestPageFromBrowser extends SpookyEnvFixture {

  import dsl._

  it("empty page") {
    val emptyPage: Doc = {
      val session = new Session(spooky)
      session.getOrProvisionWebDriver

      Snapshot(DocFilters.AcceptStatusCode2XX).apply(session).toList.head.asInstanceOf[Doc]
    }

    assert (emptyPage.findAll("div.dummy").attrs("href").isEmpty)
    assert (emptyPage.findAll("div.dummy").codes.isEmpty)
    assert (emptyPage.findAll("div.dummy").isEmpty)
  }

  it("visit, save and load") {

    val results = (
      Visit("http://en.wikipedia.org") ::
        Snapshot().as('T) :: Nil
      ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Doc]

    page.autoSave(spooky, overwrite = true)

    val loadedContent = DocUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.raw)
  }
}