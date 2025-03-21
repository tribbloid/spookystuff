package com.tribbloids.spookystuff.web.conf

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.{Doc, DocUtils}
import com.tribbloids.spookystuff.dsl
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.testutils.SpookyEnvSpec.defaultCtx
import com.tribbloids.spookystuff.web.actions.{Snapshot, Visit}
import org.apache.spark.SparkException

import java.util.Date

/**
  * Created by peng on 24/11/16.
  */
class WebDriverSpec extends SpookyBaseSpec with FileDocsFixture {

  import dsl.*

  it("empty page") {
    val emptyPage: Doc = {
      val agent = new Agent(spooky)
      agent.driverOf(Web)

      Snapshot(DocFilterImpl.AcceptStatusCode2XX).apply(agent).toList.head.asInstanceOf[Doc]
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

  it("PhantomJS DriverFactory can degrade gracefully if remote URI is unreachable") {

    val dummyPhantomJS = WebDriverFactory.PhantomJS(_ =>
      WebDriverFactory.PhantomJSDeployment(
        "dummy/file",
        "dummy.org/file"
      )
    )

    try {

      defaultCtx

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

      spooky
        .create(1 to 2)
        .fetch(_ =>
          Wget(HTML_URL) // deploy will fail, but PhantomJS won't be used
        )
        .count()

      intercept[SparkException] {
        val docs = spooky
          .create(1 to 2)
          .fetch(_ => Visit(HTML_URL))
          .map(_.observations)

        docs.collect().foreach(println)
      }
    } finally {
//      getDefaultCtx

//      spooky.getConf(Web).webDriverFactory = WebDriverFactory.PhantomJS()
    }
  }
}
