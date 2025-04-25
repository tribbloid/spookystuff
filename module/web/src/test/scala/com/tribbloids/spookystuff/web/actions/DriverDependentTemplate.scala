package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.ControlBlock.Loop
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.doc.{Doc, Observation}
import com.tribbloids.spookystuff.testutils.{BaseSpec, SpookyBaseSpec}
import com.tribbloids.spookystuff.web.agent.CleanWebDriver

abstract class DriverDependentTemplate extends SpookyBaseSpec with BaseSpec {

  import scala.concurrent.duration.*

  def driverFactory: DriverFactory[CleanWebDriver]

  describe("Visit") {

    it("+> Snapshot") {

      val results: Seq[Observation] = (
        Visit("http://www.wikipedia.org") +>
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
        Visit("http://www.wikipedia.org") +>
          WaitFor("input#searchInput").in(40.seconds) +>
          Snapshot().as("A") +>
          TextInput("input#searchInput", "Deep learning") +>
          Submit("button.pure-button") +>
          Snapshot().as("B")
      ).fetch(spooky)

      assert(results.length === 2)
      val result0 = results.head.asInstanceOf[Doc]
      val result1 = results(1).asInstanceOf[Doc]

      val id1 = Visit("http://www.wikipedia.org") ::
        WaitFor("input#searchInput") ::
        Snapshot().as("C") :: Nil
      assert(result0.uid.backtrace.repr === id1)
      assert(result0.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia</title>"))
      assert(result0.uri contains "//www.wikipedia.org")
      assert(result0.name === "A")

      val id2 = Visit("http://www.wikipedia.org") ::
        WaitFor("input#searchInput") ::
        TextInput("input#searchInput", "Deep learning") ::
        Submit("button.pure-button") ::
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
}
