package org.tribbloid.spookystuff.actions

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.entity.{Page, PageUID}
import org.tribbloid.spookystuff.factory.PageBuilder

/**
 * Created by peng on 05/06/14.
 */

//TODO: this need some serious reorganization
class TestTrace extends FunSuite {

  lazy val conf: SparkConf = new SparkConf().setAppName("dummy").setMaster("local")
  lazy val sc: SparkContext = new SparkContext(conf)
  lazy val sql: SQLContext = new SQLContext(sc)
  lazy implicit val spooky: SpookyContext = new SpookyContext(sql)
  spooky.setRoot("file://"+System.getProperty("user.home")+"/spooky-unit/")
  spooky.autoSave = false
  spooky.autoCache = false
  spooky.autoRestore = false

  override def finalize(){
    sc.stop()
  }

  import scala.concurrent.duration._

  test("visit and snapshot") {
    val builder = new PageBuilder(new SpookyContext(sql))
    Visit("http://en.wikipedia.org")(builder)
    val page = Snapshot()(builder).toList(0)
    //    val url = builder.getUrl

    assert(page.contentStr.startsWith("<!DOCTYPE html>"))
    assert(page.contentStr.contains("<title>Wikipedia"))

    assert(page.resolvedUrl.startsWith("http://en.wikipedia.org/wiki/Main_Page"))
    //    assert(url === "http://www.google.com")
  }

  test("visit, input submit and snapshot") {
    val builder = new PageBuilder(new SpookyContext(sql))
    Visit("http://www.wikipedia.org")(builder)
    TextInput("input#searchInput","Deep learning")(builder)
    Submit("input.formBtn")(builder)
    val page = Snapshot()(builder).toList(0)
    //    val url = builder.getUrl

    assert(page.contentStr.contains("<title>Deep learning - Wikipedia, the free encyclopedia</title>"))
    assert(page.resolvedUrl === "http://en.wikipedia.org/wiki/Deep_learning")
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

    val id1 = Trace(Visit("http://www.wikipedia.org")::WaitFor("input#searchInput")::Snapshot()::Nil)
    assert(res1.uid.backtrace === id1)
    assert(res1.contentStr.contains("<title>Wikipedia</title>"))
    assert(res1.resolvedUrl === "http://www.wikipedia.org/")
    assert(res1.name === "A")

    val id2 = Trace(Visit("http://www.wikipedia.org")::WaitFor("input#searchInput")::TextInput("input#searchInput","Deep learning")::Submit("input.formBtn")::Snapshot()::Nil)
    assert(res2.uid.backtrace === id2)
    assert(res2.contentStr.contains("<title>Deep learning - Wikipedia, the free encyclopedia</title>"))
    assert(res2.resolvedUrl === "http://en.wikipedia.org/wiki/Deep_learning")
    assert(res2.name === "B")
  }

  test("attributes") {
    val result = Trace(
      Visit("http://www.amazon.com") ::
        TextInput("input#twotabsearchtextbox", "Lord of the Rings") ::
        Submit("input[type=submit]") ::
        WaitFor("div#resultsCol").in(40.seconds) :: Nil
    ).autoSnapshot.resolve(spooky)

    assert(result(0).attrExist("li#result_0","id") === true)
    assert(result(0).attr1("li#result_0 dummy","title") === null)
    assert(result(0).attr1("li#result_0 h3","dummy") === null)
    assert(result(0).attr1("li#result_0 h3","class") !== null)
  }

  test("save and load") {
    val results = Trace(
      Visit("http://en.wikipedia.org") ::
      Snapshot().as('T) :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = Page.load(new Path(page1Saved.saved))(spooky)

    assert(loadedContent === page1Saved.content)
  }

  test("cache and restore") {
    val pages = Trace(
      Visit("http://en.wikipedia.org") ::
        Snapshot().as('T) :: Nil
    ).resolve(spooky)

    assert(pages.size === 1)
    val page1 = pages(0)

    assert(page1.uid === PageUID(Trace(Visit("http://en.wikipedia.org") :: Snapshot() :: Nil), Snapshot()))

    Page.autoCache(pages, page1.uid.backtrace, spooky)

    val loadedPages = Page.autoRestoreLatest(page1.uid.backtrace,spooky)

    assert(loadedPages.length === 1)

    assert(pages(0).content === loadedPages(0).content)

    assert(pages(0).copy(content = null) === loadedPages(0).copy(content = null))  }

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

  test("wget html, save and load") {
    val results = Trace(
      Wget("https://www.google.hk") :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    assert(page1.text1("title") === "Google")

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = Page.load(new Path(page1Saved.saved))(spooky)

    assert(loadedContent === page1Saved.content)
  }

  test("wget image, save and load") {
    val results = Trace(
      Wget("https://www.google.ca/images/srpr/logo11w.png") :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = Page.load(new Path(page1Saved.saved))(spooky)

    assert(loadedContent === page1Saved.content)
  }

  test("wget pdf, save and load") {
    val results = Trace(
      Wget("http://www.cs.toronto.edu/~ranzato/publications/DistBeliefNIPS2012_withAppendix.pdf") :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = Page.load(new Path(page1Saved.saved))(spooky)

    assert(loadedContent === page1Saved.content)
  }
}
