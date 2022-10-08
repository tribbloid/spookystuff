package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture

class TestPageFromFile extends TestPageFromHttp with LocalPathDocsFixture {

  it("wget dir, save and load") {
    val results = Wget(DIR_URL).fetch(spooky)

    val resultsList = results.toArray
    assert(resultsList.length === 1)
    val page = resultsList(0).asInstanceOf[Doc]

    assert(page.mimeType == "inode/directory")
    assert(page.charset.map(_.toLowerCase).get == "utf-8")
    assert(page.findAll("title").texts.isEmpty)

    assert(page.code.get.contains("<URI>file:///tmp/spookystuff/resources/testutils/files/Wikipedia.html</URI>"))

    page.autoSave(spooky, overwrite = true)

    val loadedContent = DocUtils.load(page.saved.head)(spooky)

    assert(loadedContent === page.raw)
  }
}