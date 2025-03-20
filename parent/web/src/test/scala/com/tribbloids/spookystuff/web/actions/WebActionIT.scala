package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.{Delay, OAuthV2, Trace, Wget}
import com.tribbloids.spookystuff.actions.ControlBlock.Loop
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.testutils.{BaseSpec, SpookyBaseSpec}
import com.tribbloids.spookystuff.web.agent.CleanWebDriver

// TODO: part of this test suite should be move to core
abstract class WebActionIT extends SpookyBaseSpec with BaseSpec {

  import scala.concurrent.duration.*

  def driverFactory: DriverFactory[CleanWebDriver]

  it("inject output names should change output doc names") {

    val t1 = (
      Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
        +> Snapshot().as("a")
        +> Loop(
          ClickNext("button.btn", "1" :: Nil)
            +> Delay(2.seconds)
            +> Snapshot() ~ "b"
        )
    )

    val t2 = (
      Visit("http://webscraper.io/test-sites/e-commerce/ajax/computers/laptops")
        +> Snapshot().as("c")
        +> Loop(
          ClickNext("button.btn", "1" :: Nil)
            +> Delay(2.seconds)
            +> Snapshot() ~ "d"
        )
    )

    assert(t1.exportNames === Set("c", "d"))
  }

  describe("Visit") {

    it("and Snapshot") {
      val builder = new Agent(spooky)
      Visit("http://www.wikipedia.org")(builder)
      val page = Snapshot()(builder).toList.head.asInstanceOf[Doc]

      //    assert(page.code.get.startsWith("<!DOCTYPE html>")) //not applicable to HtmlUnit
      assert(page.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia</title>"))

      assert(page.uri contains "//www.wikipedia.org/")
    }

    it("TextInput and Snapshot") {
      val results = (
        Visit("http://www.wikipedia.org") +>
          WaitFor("input#searchInput").in(40.seconds) +>
          Snapshot().as("A") +>
          TextInput("input#searchInput", "Deep learning") +>
          Submit("button.pure-button") +>
          Snapshot().as("B")
      ).fetch(spooky)

      val resultsList = results
      assert(resultsList.length === 2)
      val result0 = resultsList.head.asInstanceOf[Doc]
      val result1 = resultsList(1).asInstanceOf[Doc]

      val id1 = Visit("http://www.wikipedia.org") ::
        WaitFor("input#searchInput") ::
        Snapshot().as("C") :: Nil
      assert(result0.uid.backtrace === id1)
      assert(result0.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia</title>"))
      assert(result0.uri contains "//www.wikipedia.org")
      assert(result0.name === "A")

      val id2 = Visit("http://www.wikipedia.org") ::
        WaitFor("input#searchInput") ::
        TextInput("input#searchInput", "Deep learning") ::
        Submit("button.pure-button") ::
        Snapshot().as("D") :: Nil
      assert(result1.uid.backtrace === id2)
      assert(result1.code.get.split('\n').map(_.trim).mkString.contains("<title>Deep learning"))
      assert(result1.uri contains "//en.wikipedia.org/wiki/Deep_learning")
      assert(result1.name === "B")
    }
  }

  describe("WaitFor") {

    it("sizzle selector") {

      val results = (
        Visit("http://www.wikipedia.org/") +>
          WaitFor("a.link-box:contains(English)") +>
          Snapshot()
      ).fetch(spooky)

      val code = results.head.asInstanceOf[Doc].code.get.split('\n').map(_.trim).mkString
      assert(code.contains("Wikipedia"))
    }

    // TODO: the following 2 has external site dependencies, should be removed
    it("css selector") {

      val results = (
        Visit("http://www.wikipedia.org/") +>
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
          |  "children" : [ {
          |    "selector" : "By.sizzleCssSelector: o1",
          |    "cooldown" : "0 seconds",
          |    "blocking" : true
          |  }, {
          |    "filter" : { }
          |  } ],
          |  "limit" : 2147483647
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
            |  List
            |    Click
            |      "By.sizzleCssSelector: o1"
            |      0 seconds
            |      true
            |    Snapshot
            |      MustHaveTitle()
            |  2147483647
          """.stripMargin
        )
    }
  }
}
