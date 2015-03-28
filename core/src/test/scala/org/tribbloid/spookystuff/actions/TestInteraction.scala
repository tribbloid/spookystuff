package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.pages.Page
import org.tribbloid.spookystuff.session.DriverSession

import scala.concurrent.duration._
/**
 * Created by peng on 2/19/15.
 */
class TestInteraction extends SpookyEnvSuite {

  import org.tribbloid.spookystuff.dsl._

  test("visit and snapshot") {
    val builder = new DriverSession(spooky)
    Visit("http://en.wikipedia.org")(builder)
    val page = Snapshot()(builder).toList.head.asInstanceOf[Page]

//    assert(page.code.get.startsWith("<!DOCTYPE html>")) //not applicable to HtmlUnit
    assert(page.code.get.split('\n').map(_.trim).mkString.contains("<title>Wikipedia, the free encyclopedia</title>"))

    assert(page.uri.startsWith("http://en.wikipedia.org/wiki/Main_Page"))
  }

  test("visit, input submit and snapshot") {
    val builder = new DriverSession(spooky)
    Visit("http://www.wikipedia.org")(builder)
    TextInput("input#searchInput","Deep learning")(builder)
    Submit("input.formBtn")(builder)
    val page = Snapshot()(builder).toList.head.asInstanceOf[Page]
    //    val url = builder.getUrl

    assert(page.code.get.split('\n').map(_.trim).mkString.contains("<title>Deep learning - Wikipedia, the free encyclopedia</title>"))
    assert(page.uri === "http://en.wikipedia.org/wiki/Deep_learning")
    //    assert(url === "https://www.linkedin.com/ Input(input#first,Adam) Input(input#last,Muise) Submit(input[name=\"search\"])")
  }

  test("sizzle selector should work") {

    val results = (
      Visit("http://www.wikipedia.org/") ::
        WaitFor("a.link-box:contains(English)") ::
        Snapshot() :: Nil
    ).resolve(spooky)

    val code = results.head.asInstanceOf[Page].code.get.split('\n').map(_.trim).mkString
    assert(code.contains("<title>Wikipedia</title>"))
  }

  test("click should not double click") {
    spooky.conf.remoteResourceTimeout = 180.seconds

    try {
      val results = (Visit("https://ca.vwr.com/store/search?&pimId=582903")
        +> Paginate("a[title=Next]", delay = 2.second)).head.self.resolve(spooky)

      val numPages = results.head.asInstanceOf[Page].children("div.right a").size

      assert(results.size == numPages)
    }

    finally {
      spooky.conf.remoteResourceTimeout = 60.seconds
    }
  }

  test("dynamic paginate should returns right number of pages") {
    spooky.conf.remoteResourceTimeout = 180.seconds

    try {
      val results = (Visit("https://ca.vwr.com/store/search?label=Blotting%20Kits&pimId=3617065")
        +> Paginate("a[title=Next]", delay = 2.second)).head.self.resolve(spooky)

      val numPages = results.head.asInstanceOf[Page].children("div.right a").size

      assert(results.size == numPages)
    }

    finally {
      spooky.conf.remoteResourceTimeout = 60.seconds
    }
  }
}