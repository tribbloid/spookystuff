package com.tribbloids.spookystuff.web.conf

import com.tribbloids.spookystuff.actions.{Export, Wget}
import com.tribbloids.spookystuff.actions.Export.DocValidation.StatusCode2XX
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.{Doc, DocUtils}
import com.tribbloids.spookystuff.io.WriteMode.Overwrite
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.testutils.SpookyEnvSpec.defaultCtx
import com.tribbloids.spookystuff.web.actions.{Snapshot, Visit}

import java.util.Date

/**
  * Created by peng on 24/11/16.
  */
class WebDriverSpec extends SpookyBaseSpec with FileDocsFixture {

  it("empty page") {
    val emptyPage: Doc = {
      val agent = new Agent(spooky)
      agent.getDriver(Web)

      Snapshot().accept(StatusCode2XX).apply(agent).toList.head.asInstanceOf[Doc]
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
    page.prepareSave(spooky, Overwrite).auditing()

    val loadedContent = DocUtils.load(page.saved.head)(spooky)

    assert(loadedContent === raw)
  }

  describe("PhantomJS deployment") {

    it("can deploy normally") {

      val deployment: WebDriverFactory.PhantomJSDeployment = WebDriverFactory.PhantomJSDeployment()

      val path = deployment.deploydPath

      assert(path.endsWith("phantomjs") || path.endsWith("phantomjs.exe"))
    }

    it("can degrade gracefully if remote URI is unreachable") {

      val dummyPhantomJS = WebDriverFactory.PhantomJS(_ =>
        WebDriverFactory.PhantomJSDeployment(
          "dummy/file",
          "dummy.org/file"
        )
      )

      try {

        val spooky = defaultCtx.copy()

        spooky(Web).confUpdate(
          _.copy(
            webDriverFactory = dummyPhantomJS
          )
        )

        spooky.confUpdate(
          _.copy(
            IgnoreCachedDocsBefore = Some(new Date())
          )
        )

        Wget(HTML_URL)
          .as("T")
          .fetch(spooky) // deploy will fail, but PhantomJS won't be used

        intercept[Exception] {

          (
            Visit(HTML_URL) +>
              Snapshot().as("T")
          ).fetch(spooky)
        }
      } finally {}
    }
  }
}
