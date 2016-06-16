package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.dsl
import com.tribbloids.spookystuff.tests.LocalPathDocsFixture

class TestPageFromFile extends TestPageFromHttp with LocalPathDocsFixture {

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

    val loadedContent = DocUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.content)
  }
}