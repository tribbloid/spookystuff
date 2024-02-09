package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.testutils.FileDocsFixture

class DocSpec_File extends DocSpec with FileDocsFixture {

  describe("wget, save, load") {

    it("flat dir") {
      val results = Wget(DIR_URL).fetch(spooky)

      val resultsList = results.toArray
      assert(resultsList.length === 1)
      val page = resultsList(0).asInstanceOf[Doc]

      assert(page.mimeType == "inode/directory")
      assert(page.charsetOpt.map(_.name().toLowerCase).get == "utf-8")
      assert(page.findAll("title").texts.isEmpty)

      assert(page.code.get.contains("<URI>file:///tmp/spookystuff/resources/testutils/files/Wikipedia.html</URI>"))

      page
        .findAll("uri")
        .map(_.text.get)
        .mkString("\n")
        .shouldBe(
          """
            |
            |file:///tmp/spookystuff/resources/testutils/files
            |file:///tmp/spookystuff/resources/testutils/files/Test.pdf
            |file:///tmp/spookystuff/resources/testutils/files/Wikipedia.html
            |file:///tmp/spookystuff/resources/testutils/files/autocomplete.html
            |file:///tmp/spookystuff/resources/testutils/files/element_disappears_on_click.html
            |file:///tmp/spookystuff/resources/testutils/files/example.xml
            |file:///tmp/spookystuff/resources/testutils/files/file_upload_form.html
            |file:///tmp/spookystuff/resources/testutils/files/firebug-1.11.4.xpi
            |file:///tmp/spookystuff/resources/testutils/files/firepath-0.9.7-fx.xpi
            |file:///tmp/spookystuff/resources/testutils/files/hello_world.txt
            |file:///tmp/spookystuff/resources/testutils/files/jquery-1.8.3.js
            |file:///tmp/spookystuff/resources/testutils/files/jquery-ui-1.10.4.css
            |file:///tmp/spookystuff/resources/testutils/files/jquery-ui-1.10.4.js
            |file:///tmp/spookystuff/resources/testutils/files/logo11w.png
            |file:///tmp/spookystuff/resources/testutils/files/long_ajax_request.html
            |file:///tmp/spookystuff/resources/testutils/files/page_with_alerts.html
            |file:///tmp/spookystuff/resources/testutils/files/page_with_dynamic_select.html
            |file:///tmp/spookystuff/resources/testutils/files/page_with_frames.html
            |file:///tmp/spookystuff/resources/testutils/files/page_with_images.html
            |file:///tmp/spookystuff/resources/testutils/files/page_with_jquery.html
            |file:///tmp/spookystuff/resources/testutils/files/page_with_js_errors.html
            |file:///tmp/spookystuff/resources/testutils/files/page_with_selects_without_jquery.html
            |file:///tmp/spookystuff/resources/testutils/files/page_with_tabs.html
            |file:///tmp/spookystuff/resources/testutils/files/page_with_uploads.html
            |file:///tmp/spookystuff/resources/testutils/files/sigproc-sp.pdf
            |file:///tmp/spookystuff/resources/testutils/files/table.csv
            |file:///tmp/spookystuff/resources/testutils/files/tribbloid.json
            |""".stripMargin
        )

      val raw = page.content.blob.raw
      page.save(spooky, overwrite = true).auditing()

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
        .shouldBe(
          """
            |file:///tmp/spookystuff/resources/testutils/dir
            |file:///tmp/spookystuff/resources/testutils/dir/dir
            |file:///tmp/spookystuff/resources/testutils/dir/hivetable.csv
            |file:///tmp/spookystuff/resources/testutils/dir/table.csv
            |""".stripMargin
        )
    }
  }
}
