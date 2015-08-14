package org.tribbloid.spookystuff.pages

import org.tribbloid.spookystuff.actions.Wget
import org.tribbloid.spookystuff.{SpookyEnvSuite, dsl}

/**
 * Created by peng on 10/17/14.
 */
class TestPageFromHttp extends SpookyEnvSuite {

  import dsl._

  def htmlUrl = "http://www.wikipedia.org"
  def jsonUrl = "https://api.github.com/users/tribbloid"
  def pngUrl = "https://www.google.ca/images/srpr/logo11w.png"
  def pdfUrl = "http://stlab.adobe.com/wiki/images/d/d3/Test.pdf"

  test("wget html, save and load") {

    val results = (
      Wget(htmlUrl) :: Nil
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Page]

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
      ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Page]

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
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Page]

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
    ).resolve(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Page]

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
      ).resolve(spooky).head.asInstanceOf[Page]

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
      ).resolve(spooky).head.asInstanceOf[Page]

    val ranges = page.findAllWithSiblings("div.central-featured-lang[lang^=e]", -2 to 2)
    assert(ranges.size === 2)
    val first = ranges.head
    val second = ranges.last
    assert(first.size === 2)
    assert(first(0).attr("class").get === "central-featured-logo")
    assert(first(1).attr("lang").get === "en")
    assert(second.size === 3)
    assert(second(0).attr("lang").get === "es")
    assert(second(1).attr("lang").get === "de")
    assert(second(2).attr("lang").get === "ja")
  }
}