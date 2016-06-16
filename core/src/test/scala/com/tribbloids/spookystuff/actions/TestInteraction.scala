package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.session.DriverSession

class TestInteraction extends SpookyEnvSuite {

  import com.tribbloids.spookystuff.dsl._
  import scala.concurrent.duration._

  test("visit and snapshot") {
    val builder = new DriverSession(spooky)
    Visit("http://en.wikipedia.org")(builder)
    val page = Snapshot()(builder).toList.head.asInstanceOf[Doc]

    //    assert(page.code.get.startsWith("<!DOCTYPE html>")) //not applicable to HtmlUnit
    assert(page.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia, the free encyclopedia</title>"))

    assert(page.uri contains "//en.wikipedia.org/wiki/Main_Page")
  }

  test("visit, input submit and snapshot") {
    val results = (
      Visit("http://www.wikipedia.org") ::
        WaitFor("input#searchInput").in(40.seconds) ::
        Snapshot().as('A) ::
        TextInput("input#searchInput","Deep learning") ::
        Submit("button.pure-button") ::
        Snapshot().as('B) :: Nil
      ).fetch(spooky)

    val resultsList = results
    assert(resultsList.length === 2)
    val res1 = resultsList.head.asInstanceOf[Doc]
    val res2 = resultsList(1).asInstanceOf[Doc]

    val id1 = Visit("http://www.wikipedia.org") ::
      WaitFor("input#searchInput") ::
      Snapshot().as('C) :: Nil
    assert(res1.uid.backtrace === id1)
    assert(res1.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia</title>"))
    assert(res1.uri contains "//www.wikipedia.org")
    assert(res1.name === "A")

    val id2 = Visit("http://www.wikipedia.org") ::
      WaitFor("input#searchInput") ::
      TextInput("input#searchInput", "Deep learning") ::
      Submit("button.pure-button") ::
      Snapshot().as('D) :: Nil
    assert(res2.uid.backtrace === id2)
    assert(res2.code.get.split('\n').map(_.trim).mkString.contains("<title>Deep learning - Wikipedia, the free encyclopedia</title>"))
    assert(res2.uri contains "//en.wikipedia.org/wiki/Deep_learning")
    assert(res2.name === "B")
  }

  test("sizzle selector should work") {

    val results = (
      Visit("http://www.wikipedia.org/") ::
        WaitFor("a.link-box:contains(English)") ::
        Snapshot() :: Nil
      ).fetch(spooky)

    val code = results.head.asInstanceOf[Doc].code.get.split('\n').map(_.trim).mkString
    assert(code.contains("Wikipedia"))
  }

//  test("visit should handle corsera") {

    //TODO: PhantomJS is broken on this: re-enable after its fixed or switching to alternative browser.

    //    val results = (
    //      Visit("https://www.coursera.org/yale") ::
    //        Snapshot() :: Nil
    //      ).resolve(spooky)
    //
    //    val code = results.head.asInstanceOf[Page].code.get.split('\n').map(_.trim).mkString
    //    assert(code.contains("<title>Yale University"))
//  }
}