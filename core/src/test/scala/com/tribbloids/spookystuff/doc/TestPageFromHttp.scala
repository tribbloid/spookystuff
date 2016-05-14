package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.{SpookyEnvSuite, dsl}
import org.apache.commons.csv.CSVFormat

/**
 * Created by peng on 10/17/14.
 */
class TestPageFromHttp extends SpookyEnvSuite {

  import dsl._

  def htmlUrl = "http://tribbloid.github.io/spookystuff/test/Wikipedia.html"
  def jsonUrl = "http://tribbloid.github.io/spookystuff/test/tribbloid.json"
  //TODO: add this after fetch can semi-auto-detect content-type
  def jsonUrlIncorrectContentType = "https://raw.githubusercontent.com/tribbloid/spookystuff/master/core/src/test/resources/site/tribbloid.json"
  def pngUrl = "https://www.google.ca/images/srpr/logo11w.png"
  def pdfUrl = "http://stlab.adobe.com/wiki/images/d/d3/Test.pdf"
  def xmlUrl = "http://tribbloid.github.io/spookystuff/test/pom.xml"
  def csvUrl = "http://tribbloid.github.io/spookystuff/test/table.csv"

  test("wget html, save and load") {

    val results = (
      Wget(htmlUrl) :: Nil
    ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Doc]

    assert(page.mimeType == "text/html")
    assert(page.charset.map(_.toLowerCase).get == "utf-8")
    assert(page.findAll("title").texts.head startsWith "Wikipedia")

    page.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }

  test("wget json, save and load") {

    val results = (
      Wget(jsonUrl) :: Nil
      ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Doc]

    assert(page.mimeType == "application/json")
    assert(page.charset.map(_.toLowerCase).get == "utf-8")
    assert(page.\("html_url").texts == "https://github.com/tribbloid" :: Nil)
    assert(page.\\("html_url").texts == "https://github.com/tribbloid" :: Nil)

    assert(page.\("notexist").isEmpty)
    assert(page.\\("notexist").isEmpty)

    page.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }

  test("wget image, save and load") {

    val results = (
      Wget(pngUrl) :: Nil
    ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Doc]

    assert(page.mimeType == "image/png")
    assert(page.charset.map(_.toLowerCase).get == "utf-8")
    assert(page.findAll("title").text.get == "")

    page.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }

  test("wget pdf, save and load") {

    val results = (
      Wget(pdfUrl) :: Nil
    ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Doc]

    assert(page.mimeType == "application/pdf")
    assert(page.charset.map(_.toLowerCase).get == "utf-8")
    assert(page.findAll("title").text.get == "Microsoft Word - Document1")

    page.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }

  test("childrenWithSiblings") {
    val page = (
      Wget(htmlUrl) :: Nil
      ).fetch(spooky).head.asInstanceOf[Doc]

    val ranges = page.findAllWithSiblings("a.link-box em", -2 to 1)
    assert(ranges.size === 10)
    val first = ranges.head
    assert(first.size === 4)
    assert(first.head.code.get.startsWith("<strong"))
    assert(first(1).code.get.startsWith("<br"))
    assert(first(2).code.get.startsWith("<em"))
    assert(first(3).code.get.startsWith("<br"))
  }

  test("childrenWithSiblings with overlapping elimiation") {
    val page = (
      Wget(htmlUrl) :: Nil
      ).fetch(spooky).head.asInstanceOf[Doc]

    val ranges = page.findAllWithSiblings("div.central-featured-lang[lang^=e]", -2 to 2)
    assert(ranges.size === 2)
    val first = ranges.head
    val second = ranges.last
    assert(first.size === 2)
    assert(first(0).attr("class").get === "central-featured-logo")
    assert(first(1).attr("lang").get === "en")
    assert(second.size === 3)
    assert(second(0).attr("class").get.contains("lang"))
    assert(second(1).attr("class").get.contains("lang"))
    assert(second(2).attr("class").get.contains("lang"))
  }

  test("wget xml, save and load") {

    val results = (
      Wget(xmlUrl) :: Nil
      ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Doc]

    assert(page.mimeType == "application/xml")
    assert(page.charset.map(_.toLowerCase).get == "utf-8")
    assert(page.findAll("title").texts.isEmpty)

    assert(page.findAll("profiles > profile").size == 5)

//        page.findAll("*").flatMap(_.breadcrumb).map(_.mkString("/")).distinct.foreach(println)

    page.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }

  test("wget csv, save and load") {

    val results = (
      Wget(csvUrl) :: Nil
      ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Doc].set("csvFormat" -> CSVFormat.newFormat('\t').withQuote('"').withHeader())

    assert(page.mimeType == "text/csv")
    assert(Set("iso-8859-1", "utf-8") contains page.charset.map(_.toLowerCase).get) //the file is just using ASCII chars
    assert(page.findAll("title").texts.isEmpty)

    assert(page.findAll("Name").size == 14)

    //    page.findAll("*").flatMap(_.breadcrumb).map(_.mkString("/")).distinct.foreach(println)

    page.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }
}