package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.dsl

/**
 * Created by peng on 27/07/15.
 */
class TestPageFromFile extends TestPageFromHttp {

  override def htmlUrl = this.getClass.getClassLoader.getResource("site/Wikipedia.html").getPath
  override def jsonUrl = this.getClass.getClassLoader.getResource("site/tribbloid.json").getPath
  override def pngUrl = this.getClass.getClassLoader.getResource("site/logo11w.png").getPath
  override def pdfUrl = this.getClass.getClassLoader.getResource("site/Test.pdf").getPath
  override def xmlUrl = this.getClass.getClassLoader.getResource("site/pom.xml").getPath
  override def csvUrl = this.getClass.getClassLoader.getResource("site/table.csv").getPath

  def dirUrl = this.getClass.getClassLoader.getResource("site").getPath

  import dsl._

  test("wget dir, save and load") {
    val results = (
      Wget(dirUrl) :: Nil
      ).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Doc]

    page.code.foreach(println)

    assert(page.mimeType == "inode/directory")
    assert(page.charset.map(_.toLowerCase).get == "utf-8")
    assert(page.findAll("title").texts.isEmpty)

    page.autoSave(spooky,overwrite = true)

    val loadedContent = PageUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }
}