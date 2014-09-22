package org.tribbloid.spookystuff.entity

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.tribbloid.spookystuff.{Utils, SpookyContext}
import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.factory.PageBuilder

/**
 * Created by peng on 05/06/14.
 */

//TODO: this need some serious reorganization
class TestPageBuilder extends FunSuite {

  lazy val conf: SparkConf = new SparkConf().setAppName("dummy").setMaster("local")
  lazy val sc: SparkContext = new SparkContext(conf)
  lazy val sql: SQLContext = new SQLContext(sc)
  lazy implicit val spooky: SpookyContext = new SpookyContext(sql)
  spooky.setRoot("file://"+System.getProperty("user.home")+"/spOOky/"+"unit")
  spooky.autoSave = false
  spooky.autoCache = false
  spooky.autoRestore = false

  import scala.concurrent.duration._

  test("visit and snapshot") {
    val builder = new PageBuilder(new SpookyContext(null: SQLContext))()
    Visit("http://en.wikipedia.org").doExe(builder)
    val page = Snapshot().doExe(builder).toList(0)
    //    val url = builder.getUrl

    assert(page.contentStr.startsWith("<!DOCTYPE html>"))
    assert(page.contentStr.contains("<title>Wikipedia"))

    assert(page.resolvedUrl.startsWith("http://en.wikipedia.org/wiki/Main_Page"))
    //    assert(url === "http://www.google.com")
  }

  test("visit, input submit and snapshot") {
    val builder = new PageBuilder(new SpookyContext(null: SQLContext))()
    Visit("https://www.linkedin.com/").doExe(builder)
    TextInput("input#first","Adam").doExe(builder)
    TextInput("input#last","Muise").doExe(builder)
    Submit("input[name=\"search\"]").doExe(builder)
    val page = Snapshot().doExe(builder).toList(0)
    //    val url = builder.getUrl

    assert(page.contentStr.contains("<title>Adam Muise profiles | LinkedIn</title>"))
    assert(page.resolvedUrl === "https://www.linkedin.com/pub/dir/?first=Adam&last=Muise")
    //    assert(url === "https://www.linkedin.com/ Input(input#first,Adam) Input(input#last,Muise) Submit(input[name=\"search\"])")
  }

  test("resolve") {
    val results = PageBuilder.resolve(
      Visit("https://www.linkedin.com/") ::
      DelayFor("input[name=\"search\"]").in(40.seconds) ::
      Snapshot().as("A") ::
      TextInput("input#first","Adam") ::
      TextInput("input#last","Muise") ::
      Submit("input[name=\"search\"]") ::
      Snapshot().as("B") :: Nil,
      dead = false
    )(spooky)

    val resultsList = results
    assert(resultsList.length === 2)
    val res1 = resultsList(0)
    val res2 = resultsList(1)

    val id1 = Seq[Action](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]"), Snapshot())
    assert(res1.backtrace === id1)
    assert(res1.contentStr.contains("<title>World's Largest Professional Network | LinkedIn</title>"))
    assert(res1.resolvedUrl === "https://www.linkedin.com/")
//    assert(res1.alias === "A")

    val id2 = Seq[Action](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]"), TextInput("input#first","Adam"),TextInput("input#last","Muise"),Submit("input[name=\"search\"]"), Snapshot())
    assert(res2.backtrace === id2)
    assert(res2.contentStr.contains("<title>Adam Muise profiles | LinkedIn</title>"))
    assert(res2.resolvedUrl === "https://www.linkedin.com/pub/dir/?first=Adam&last=Muise")
//    assert(res2.alias === "B")
  }

  test("extract") {
    val result = PageBuilder.resolve(
      Visit("https://www.linkedin.com/") ::
      DelayFor("input[name=\"search\"]").in(40.seconds) ::
      TextInput("input#first", "Adam") ::
      TextInput("input#last", "Muise") ::
      Submit("input[name=\"search\"]") :: Nil,
      dead = false
    )(spooky)

    val id = Seq[Action](Visit("https://www.linkedin.com/"), DelayFor("input[name=\"search\"]"), TextInput("input#first","Adam"),TextInput("input#last","Muise"),Submit("input[name=\"search\"]"), Snapshot())
    assert(result(0).backtrace === id)
    assert(result(0).contentStr.contains("<title>Adam Muise profiles | LinkedIn</title>"))
    assert(result(0).resolvedUrl === "https://www.linkedin.com/pub/dir/?first=Adam&last=Muise")
  }

  test("attributes") {
    val result = PageBuilder.resolve(
      Visit("http://www.amazon.com/") ::
      TextInput("input#twotabsearchtextbox", "Lord of the Rings") ::
      Submit("input.nav-submit-input") ::
      DelayFor("div#resultsCol").in(40.seconds) :: Nil,
      dead = false
    )(spooky)

    assert(result(0).attrExist("div#result_0 h3 span.bold","title") === true)
    assert(result(0).attr1("div#result_0 h3 span.dummy","title") === null)
    assert(result(0).attr1("div#result_0 h3 span.bold","dummy") === null)
  }

  test("save and load") {
    val results = PageBuilder.resolve(
      Visit("https://www.linkedin.com/") ::
      Snapshot().as("T") :: Nil,
      dead = false
    )(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = PageBuilder.load(new Path(page1Saved.saved))(spooky.hConf)

    assert(loadedContent === page1Saved.content)
  }

  test("cache and restore") {
    val results = PageBuilder.resolve(
      Visit("https://www.linkedin.com/") ::
        Snapshot().as("T") :: Nil,
      dead = false
    )(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Cached = page1.autoCache(spooky,overwrite = true)

    val uid = PageUID( Visit("https://www.linkedin.com/") :: Snapshot() :: Nil)

    val cachedPath = new Path(
      Utils.urlConcat(
        spooky.autoCacheRoot,
        spooky.PagePathLookup(uid).toString
      )
    )

    val loadedPage = PageBuilder.restoreLatest(cachedPath)(spooky.hConf)(0)

    assert(page1Cached.content === loadedPage.content)

    assert(page1Cached.copy(content = null) === loadedPage.copy(content = null))
  }

  test("cache multiple pages and restore") {
    val results = PageBuilder.resolve(
      Visit("https://www.linkedin.com/") ::
        Snapshot().as("T") :: Nil,
      dead = false
    )(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Cached = page1.autoCache(spooky,overwrite = true)

    val uid = PageUID( Visit("https://www.linkedin.com/") :: Snapshot() :: Nil)

    val cachedPath = new Path(
      Utils.urlConcat(
        spooky.autoCacheRoot,
        spooky.PagePathLookup(uid).toString
      )
    )

    val loadedPage = PageBuilder.restoreLatest(cachedPath)(spooky.hConf)(0)

    assert(page1Cached.content === loadedPage.content)

    assert(page1Cached.copy(content = null) === loadedPage.copy(content = null))
  }

  test("wget html, save and load") {
    val results = PageBuilder.resolve(
      Wget("https://www.google.hk") :: Nil,
      dead = false
    )(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    assert(page1.text1("title") === "Google")

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = PageBuilder.load(new Path(page1Saved.saved))(spooky.hConf)

    assert(loadedContent === page1Saved.content)
  }

  test("wget image, save and load") {
    val results = PageBuilder.resolve(
      Wget("http://col.stb01.s-msn.com/i/74/A177116AA6132728F299DCF588F794.gif") :: Nil,
      dead = false
    )(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = PageBuilder.load(new Path(page1Saved.saved))(spooky.hConf)

    assert(loadedContent === page1Saved.content)
  }

  test("wget pdf, save and load") {
    val results = PageBuilder.resolve(
      Wget("http://www.cs.toronto.edu/~ranzato/publications/DistBeliefNIPS2012_withAppendix.pdf") :: Nil,
      dead = false
    )(spooky)

    val resultsList = results.toArray
    assert(resultsList.size === 1)
    val page1 = resultsList(0)

    val page1Saved = page1.autoSave(spooky,overwrite = true)

    val loadedContent = PageBuilder.load(new Path(page1Saved.saved))(spooky.hConf)

    assert(loadedContent === page1Saved.content)
  }

}
