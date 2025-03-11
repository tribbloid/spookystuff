package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.Wget
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
      assert(page.find("title").texts.isEmpty)

      page
        .find("uri")
        .map(_.text.get)
        .mkString("\n")
        .stripTmpRoot
        .shouldBe(
          """
            |file:///testutils/files
            |file:///testutils/files/Test.pdf
            |file:///testutils/files/Wikipedia.html
            |file:///testutils/files/autocomplete.html
            |file:///testutils/files/element_disappears_on_click.html
            |file:///testutils/files/example.xml
            |file:///testutils/files/file_upload_form.html
            |file:///testutils/files/firebug-1.11.4.xpi
            |file:///testutils/files/firepath-0.9.7-fx.xpi
            |file:///testutils/files/hello_world.txt
            |file:///testutils/files/jquery-1.8.3.js
            |file:///testutils/files/jquery-ui-1.10.4.css
            |file:///testutils/files/jquery-ui-1.10.4.js
            |file:///testutils/files/logo11w.png
            |file:///testutils/files/long_ajax_request.html
            |file:///testutils/files/page_with_alerts.html
            |file:///testutils/files/page_with_dynamic_select.html
            |file:///testutils/files/page_with_frames.html
            |file:///testutils/files/page_with_images.html
            |file:///testutils/files/page_with_jquery.html
            |file:///testutils/files/page_with_js_errors.html
            |file:///testutils/files/page_with_selects_without_jquery.html
            |file:///testutils/files/page_with_tabs.html
            |file:///testutils/files/page_with_uploads.html
            |file:///testutils/files/sigproc-sp.pdf
            |file:///testutils/files/table.csv
            |file:///testutils/files/tribbloid.json
            |""".stripMargin
        )

      val raw = page.content.blob.raw
      page.prepareSave(spooky, overwrite = true).auditing()

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
      assert(page.find("title").texts.isEmpty)

      page
        .find("uri")
        .map(_.text.get)
        .mkString("\n")
        .stripTmpRoot
        .shouldBe(
          """
            |file:///testutils/dir
            |file:///testutils/dir/dir
            |file:///testutils/dir/hivetable.csv
            |file:///testutils/dir/table.csv
            |""".stripMargin
        )
    }
  }
}
