package com.tribbloids.spookystuff.doc

import ai.acyclic.prover.commons.spark.serialization.AssertSerializable
import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

/**
  * Created by peng on 11/30/14.
  */
class UnstructuredSpec extends SpookyBaseSpec {

  val resources: FileDocsFixture.type = FileDocsFixture
  import resources.*

  @transient lazy val page: Doc = Wget(HTML_URL).as("old").fetch(spooky).head.asInstanceOf[Doc]

  describe("is serializable for") {

    it("div") {
      val elements = page.find("div.central-featured-lang")

      assert(elements.size === 10)

      elements.foreach { element =>
        AssertSerializable[Unstructured](element).on(
          condition = { (element, element2) =>
            assert(element === element2)
            assert(
              element.asInstanceOf[HtmlElement].formattedCode.get.split("\n").map(_.trim) === element2
                .asInstanceOf[HtmlElement]
                .formattedCode
                .get
                .split("\n")
                .map(_.trim)
            )
            assert(element.find("a").size === element2.find("a").size)
            assert(element.attr("class") === element2.attr("class"))
            assert(element.code === element2.code)
            assert(element.ownText === element2.ownText)
            assert(element.boilerPipe === element2.boilerPipe)
          }
        )
      }
    }
  }

  it("attrs should handles empty attributes properly") {

    assert(page.find("h1.central-textlogo img").attrs("title").nonEmpty)
    assert(page.find("h1.central-textlogo img dummy").attrs("title").isEmpty)
    assert(page.find("h1.central-textlogo img").attrs("dummy").isEmpty)
  }
}
