package com.tribbloids.spookystuff.pages

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.dsl

/**
 * Created by peng on 13/08/15.
 */
class TestPageFromAbsoluteFile extends TestPageFromFile {

  override def htmlUrl = "file://" + super.htmlUrl
  override def jsonUrl = "file://" + super.jsonUrl
  override def pngUrl = "file://" + super.pngUrl
  override def pdfUrl = "file://" + super.pdfUrl
  override def xmlUrl = "file://" + super.xmlUrl

  import dsl._

  test("wget xml, save and load") {

    val results = (
      Wget(xmlUrl) :: Nil
      ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Page]

    assert(page.mimeType == "application/xml")
    assert(page.charset.map(_.toLowerCase).get == "utf-8")
    assert(page.findAll("title").texts.isEmpty)

    page.findAll("*").flatMap(_.breadcrumb).map(_.mkString("/")).distinct.foreach(println)

    page.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }
}
