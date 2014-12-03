package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.SparkEnvSuite
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.session.Session

/**
 * Created by peng on 05/06/14.
 */

//TODO: this need some serious reorganization
class TestTrace extends SparkEnvSuite {

  import scala.concurrent.duration._

  test("visit and snapshot") {
    val builder = new Session(spooky)
    Visit("http://en.wikipedia.org")(builder)
    val page = Snapshot()(builder).toList(0)
    //    val url = builder.getUrl

    assert(page.markup.get.startsWith("<!DOCTYPE html>"))
    assert(page.markup.get.contains("<title>Wikipedia"))

    assert(page.uri.startsWith("http://en.wikipedia.org/wiki/Main_Page"))
    //    assert(url === "http://www.google.com")
  }

  test("visit, input submit and snapshot") {
    val builder = new Session(spooky)
    Visit("http://www.wikipedia.org")(builder)
    TextInput("input#searchInput","Deep learning")(builder)
    Submit("input.formBtn")(builder)
    val page = Snapshot()(builder).toList(0)
    //    val url = builder.getUrl

    assert(page.markup.get.contains("<title>Deep learning - Wikipedia, the free encyclopedia</title>"))
    assert(page.uri === "http://en.wikipedia.org/wiki/Deep_learning")
    //    assert(url === "https://www.linkedin.com/ Input(input#first,Adam) Input(input#last,Muise) Submit(input[name=\"search\"])")
  }

  test("resolve") {
    val results = Trace(
      Visit("http://www.wikipedia.org") ::
        WaitFor("input#searchInput").in(40.seconds) ::
        Snapshot().as('A) ::
        TextInput("input#searchInput","Deep learning") ::
        Submit("input.formBtn") ::
        Snapshot().as('B) :: Nil
    ).resolve(spooky)

    val resultsList = results
    assert(resultsList.length === 2)
    val res1 = resultsList(0)
    val res2 = resultsList(1)

    val id1 = Trace(Visit("http://www.wikipedia.org")::WaitFor("input#searchInput")::Snapshot().as('C)::Nil)
    assert(res1.uid.backtrace === id1)
    assert(res1.markup.get.contains("<title>Wikipedia</title>"))
    assert(res1.uri === "http://www.wikipedia.org/")
    assert(res1.name === "A")

    val id2 = Trace(Visit("http://www.wikipedia.org")::WaitFor("input#searchInput")::TextInput("input#searchInput","Deep learning")::Submit("input.formBtn")::Snapshot().as('D)::Nil)
    assert(res2.uid.backtrace === id2)
    assert(res2.markup.get.contains("<title>Deep learning - Wikipedia, the free encyclopedia</title>"))
    assert(res2.uri === "http://en.wikipedia.org/wiki/Deep_learning")
    assert(res2.name === "B")
  }

  test("attributes") {
    val result = Trace(
      Visit("http://www.amazon.com") ::
        TextInput("input#twotabsearchtextbox", "Lord of the Rings") ::
        Submit("input[type=submit]") ::
        WaitFor("div#resultsCol").in(40.seconds) :: Nil
    ).autoSnapshot.resolve(spooky)

    assert(result(0)("li#result_0").attrs("id").nonEmpty)
    assert(result(0)("li#result_0 dummy").attrs("title").isEmpty)
    assert(result(0)("li#result_0 h3").attrs("dummy").isEmpty)
    assert(result(0)("li#result_0 h3").attrs("class").nonEmpty)
  }

//  test("cache multiple pages and restore") {
//    val results = PageBuilder.resolve(
//      Visit("https://www.linkedin.com/") ::
//        Snapshot().as("T") :: Nil,
//      dead = false
//    )(spooky)
//
//    val resultsList = results.toArray
//    assert(resultsList.size === 1)
//    val page1 = resultsList(0)
//
//    val page1Cached = page1.autoCache(spooky,overwrite = true)
//
//    val uid = PageUID( Visit("https://www.linkedin.com/") :: Snapshot() :: Nil)
//
//    val cachedPath = new Path(
//      Utils.urlConcat(
//        spooky.autoCacheRoot,
//        spooky.PagePathLookup(uid).toString
//      )
//    )
//
//    val loadedPage = PageBuilder.restoreLatest(cachedPath)(spooky.hConf)(0)
//
//    assert(page1Cached.content === loadedPage.content)
//
//    assert(page1Cached.copy(content = null) === loadedPage.copy(content = null))
//  }
}
