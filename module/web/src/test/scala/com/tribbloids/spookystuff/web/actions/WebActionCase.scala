package com.tribbloids.spookystuff.web.actions

import ai.acyclic.prover.commons.spark.TestHelper
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.ControlBlock.Loop
import com.tribbloids.spookystuff.actions.Export.DocValidation.StatusCode2XX
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.doc.{Doc, DocUtils, Observation}
import com.tribbloids.spookystuff.io.WriteMode.Overwrite
import com.tribbloids.spookystuff.testutils.{BaseSpec, FileURIDocsFixture}
import com.tribbloids.spookystuff.web.agent.CleanWebDriver
import com.tribbloids.spookystuff.web.conf.Web

abstract class WebActionCase extends BaseSpec with FileURIDocsFixture {

  import scala.concurrent.duration.*

  def driverFactory: DriverFactory[CleanWebDriver]

  lazy val spooky: SpookyContext = {

    val webConf = Web.Conf(driverFactory)

    new SpookyContext(
      TestHelper.TestSparkSession
    )
      .setConf(webConf)
  }

  object TaskLocal extends WebActionCase {

    override lazy val suiteName: String = WebActionCase.this.suiteName + "(Task Local)"

    override lazy val driverFactory: DriverFactory.TaskLocal[CleanWebDriver] =
      WebActionCase.this.driverFactory.taskLocal
  }

  it("empty page") {
    val emptyPage: Doc = {
      val agent = new Agent(spooky)

      Snapshot().accept(StatusCode2XX).apply(agent).toList.head.asInstanceOf[Doc]
    }

    assert(emptyPage.findAll("div.dummy").attrs("href").isEmpty)
    assert(emptyPage.findAll("div.dummy").codes.isEmpty)
    assert(emptyPage.findAll("div.dummy").isEmpty)
  }

  describe("Visit") {

    it("+> Snapshot") {

      val results: Seq[Observation] = (
        Visit("https://www.wikipedia.org") +>
          WaitFor("input#searchInput").in(40.seconds) +>
          Snapshot()
      ).fetch(spooky)

      assert(results.size == 1)
      val page = results.head.asInstanceOf[Doc]

      //    assert(page.code.get.startsWith("<!DOCTYPE html>")) //not applicable to HtmlUnit
      assert(page.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia</title>"))

      assert(page.uri contains "//www.wikipedia.org/")
    }

    it("+> TextInput +> Snapshot") {
      val results: Seq[Observation] = (
        Visit("https://www.wikipedia.org") +>
          WaitFor("input#searchInput").in(40.seconds) +>
          Snapshot().as("A") +>
          TextInput("input#searchInput", "Deep learning") +>
          Submit("button.pure-button") +>
          WaitFor("h1#firstHeading").in(40.seconds) +>
          Snapshot().as("B")
      ).fetch(spooky)

      assert(results.length === 2)
      val result0 = results.head.asInstanceOf[Doc]
      val result1 = results(1).asInstanceOf[Doc]

      val id1 = Visit("https://www.wikipedia.org") ::
        WaitFor("input#searchInput") ::
        Snapshot().as("C") :: Nil
      assert(result0.uid.backtrace.repr === id1)
      assert(result0.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia</title>"))
      assert(result0.uri contains "//www.wikipedia.org")
      assert(result0.name === "A")

      val id2 = Visit("https://www.wikipedia.org") ::
        WaitFor("input#searchInput") ::
        TextInput("input#searchInput", "Deep learning") ::
        Submit("button.pure-button") ::
        WaitFor("h1#firstHeading") ::
        Snapshot().as("D") :: Nil
      assert(result1.uid.backtrace.repr === id2)
      assert(result1.code.get.split('\n').map(_.trim).mkString.contains("<title>Deep learning"))
      assert(result1.uri contains "//en.wikipedia.org/wiki/Deep_learning")
      assert(result1.name === "B")
    }
  }

  describe("WaitFor") {

    it("sizzle selector") {

      val results = (
        Visit("https://www.wikipedia.org/") +>
          WaitFor("a.link-box:contains(English)") +>
          Snapshot()
      ).fetch(spooky)

      val code = results.head.asInstanceOf[Doc].code.get.split('\n').map(_.trim).mkString
      assert(code.contains("Wikipedia"))
    }

    // TODO: the following 2 has external site dependencies, should be removed
    it("css selector") {

      val results = (
        Visit("https://www.wikipedia.org/") +>
          WaitFor("cssSelector: a.link-box") +>
          Snapshot()
      ).fetch(spooky)

      val code = results.head.asInstanceOf[Doc].code.get.split('\n').map(_.trim).mkString
      assert(code.contains("Wikipedia"))
    }
  }

  describe("Click") {
    val action = Click("o1")

    it("-> JSON") {
      val str = action.prettyJSON() // TODO: add as a trait
      str.shouldBe(
        """
          |{
          |  "selector" : "By.sizzleCssSelector: o1",
          |  "cooldown" : "0 seconds",
          |  "blocking" : true
          |}
        """.stripMargin
      )
    }

    it("-> treeText") {
      val str = action.treeText // TODO: add as a trait

      str
        .replaceAllLiterally("com.tribbloids.spookystuff.actions.", "")
        .shouldBe(
          """
            |Click
            |  "By.sizzleCssSelector: o1"
            |  0 seconds
            |  true
          """.stripMargin
        )
    }
  }

  describe("Loop") {
    val action = Loop(
      Click("o1")
        +> Snapshot()
    )

    it("-> JSON") {
      val str = action.prettyJSON()
      str.shouldBe(
        """
          |{
          |  "trace" : {
          |    "repr" : [ {
          |      "selector" : "By.sizzleCssSelector: o1",
          |      "cooldown" : "0 seconds",
          |      "blocking" : true
          |    }, { } ]
          |  },
          |  "limit" : 16
          |}
        """.stripMargin
      )
    }

    it("-> treeText") {
      val str = action.treeText
      str
        .replaceAllLiterally("com.tribbloids.spookystuff.actions.", "")
        .shouldBe(
          """
            |Loop
            |  Trace
            |    List
            |      Click
            |        "By.sizzleCssSelector: o1"
            |        0 seconds
            |        true
            |      Snapshot
            |  16
          """.stripMargin
        )
    }
  }

  it(" save and load") {

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
}
