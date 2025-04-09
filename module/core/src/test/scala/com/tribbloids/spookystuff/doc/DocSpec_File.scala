package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.io.WriteMode.Overwrite
import com.tribbloids.spookystuff.testutils.FileDocsFixture

class DocSpec_File extends DocSpec {

  override val resources: FileDocsFixture.type = FileDocsFixture
  import resources.*

  describe("wget, save, load") {

    it("flat dir") {
      val results = Wget(DIR_URL).fetch(spooky)

      val resultsList = results.toArray
      assert(resultsList.length === 1)
      val page = resultsList(0).asInstanceOf[Doc]

      assert(page.mimeType == "inode/directory")
      assert(page.charsetOpt.map(_.name().toLowerCase).get == "utf-8")
      assert(page.findAll("title").texts.isEmpty)

      page
        .findAll("uri")
        .map(_.text.get)
        .mkString("\n")
        .stripTmpRoot
        .shouldBe(
          """
            |file:///testutils/dir/dir/dir/dir
            |file:///testutils/dir/dir/dir/dir/tribbloid.json
            |""".stripMargin
        )

      val raw = page.content.blob.raw
      page.prepareSave(spooky, Overwrite).auditing()

      val loadedContent = DocUtils.load(page.saved.head)(spooky)

      assert(loadedContent === raw)
    }

    it("deep dir") {
      val results = Wget(DEEP_DIR_URL).fetch(spooky)

      val resultsList = results.toArray
      assert(resultsList.length === 1)
      val page = resultsList(0).asInstanceOf[Doc]

      assert(page.mimeType == "inode/directory")
      assert(page.charsetOpt.map(_.name().toLowerCase).get == "utf-8")
      assert(page.findAll("title").texts.isEmpty)

      page
        .findAll("uri")
        .map(_.text.get)
        .mkString("\n")
        .stripTmpRoot
        .shouldBe(
          """
            |file:///testutils/dir
            |file:///testutils/dir/dir
            |file:///testutils/dir/Test.pdf
            |file:///testutils/dir/Wikipedia.html
            |file:///testutils/dir/example.xml
            |file:///testutils/dir/hivetable.csv
            |file:///testutils/dir/logo11w.png
            |file:///testutils/dir/table.csv
            |file:///testutils/dir/tribbloid.json
            |""".stripMargin
        )
    }
  }
}
