package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.pages.Page
import com.tribbloids.spookystuff.session.DriverSession

import scala.concurrent.duration._
/**
 * Created by peng on 2/19/15.
 */
class TestInteraction extends SpookyEnvSuite {

  import com.tribbloids.spookystuff.dsl._

  test("visit and snapshot") {
    val builder = new DriverSession(spooky)
    Visit("http://en.wikipedia.org")(builder)
    val page = Snapshot()(builder).toList.head.asInstanceOf[Page]

//    assert(page.code.get.startsWith("<!DOCTYPE html>")) //not applicable to HtmlUnit
    assert(page.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia, the free encyclopedia</title>"))

    assert(page.uri contains "//en.wikipedia.org/wiki/Main_Page")
  }

  test("visit, input submit and snapshot") {
    val builder = new DriverSession(spooky)
    Visit("http://www.wikipedia.org")(builder)
    TextInput("input#searchInput","Deep learning")(builder)
    Submit("button.formBtn")(builder)
    val page = Snapshot()(builder).toList.head.asInstanceOf[Page]
    //    val url = builder.getUrl

    assert(page.code.get.split('\n').map(_.trim).mkString.contains("<title>Deep learning - Wikipedia, the free encyclopedia</title>"))
    assert(page.uri contains "//en.wikipedia.org/wiki/Deep_learning")
    //    assert(url === "https://www.linkedin.com/ Input(input#first,Adam) Input(input#last,Muise) Submit(input[name=\"search\"])")
  }

  test("sizzle selector should work") {

    val results = (
      Visit("http://www.wikipedia.org/") ::
        WaitFor("a.link-box:contains(English)") ::
        Snapshot() :: Nil
    ).fetch(spooky)

    val code = results.head.asInstanceOf[Page].code.get.split('\n').map(_.trim).mkString
    assert(code.contains("<title>Wikipedia</title>"))
  }

  test("visit should handle corsera") {

    //TODO: PhantomJS is broken on this: re-enable after its fixed or switching to alternative browser.

//    val results = (
//      Visit("https://www.coursera.org/yale") ::
//        Snapshot() :: Nil
//      ).resolve(spooky)
//
//    val code = results.head.asInstanceOf[Page].code.get.split('\n').map(_.trim).mkString
//    assert(code.contains("<title>Yale University"))
  }
}