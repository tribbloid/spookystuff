package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.doc.{Doc, DocUtils}
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.web.conf.Web
import com.tribbloids.spookystuff.dsl
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

class WebDriverSpec extends SpookyBaseSpec with FileDocsFixture {

  import dsl._

  it("empty page") {
    val emptyPage: Doc = {
      val session = new Agent(spooky)
      session.driverOf(Web)

      Snapshot(DocFilterImpl.AcceptStatusCode2XX).apply(session).toList.head.asInstanceOf[Doc]
    }

    assert(emptyPage.findAll("div.dummy").attrs("href").isEmpty)
    assert(emptyPage.findAll("div.dummy").codes.isEmpty)
    assert(emptyPage.findAll("div.dummy").isEmpty)
  }

  it("visit, save and load") {

    val results = (
      Visit(HTML_URL) +>
        Snapshot().as("T")
    ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Doc]

    val raw = page.blob.raw
    page.prepareSave(spooky, overwrite = true).auditing()

    val loadedContent = DocUtils.load(page.saved.head)(spooky)

    assert(loadedContent === raw)
  }
}
