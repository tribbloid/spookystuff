package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable
import org.apache.spark.SparkEnv

/**
  * Created by peng on 11/30/14.
  */
class TestUnstructured extends SpookyEnvFixture with LocalPathDocsFixture {

  lazy val page: Doc = Wget(HTML_URL).as('old).fetch(spooky).head.asInstanceOf[Doc]

  it("Unstructured is serializable for div") {
    val elements = page.findAll("div.central-featured-lang")

    assert(elements.size === 10)

    elements.foreach { element =>
      AssertSerializable[Unstructured](
        element,
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
          assert(element.findAll("a").size === element2.findAll("a").size)
          assert(element.attr("class") === element2.attr("class"))
          assert(element.code === element2.code)
          assert(element.ownText === element2.ownText)
          assert(element.boilerPipe === element2.boilerPipe)
        }
      )

    }
  }

  lazy val tablePage = Wget(HTML_URL).as('old).fetch(spooky).head.asInstanceOf[Doc]

  ignore("Unstructured is serializable for tr") {
    val elements = tablePage.findAll("table#mp-topbanner > tbody > tr")

    assert(elements.size === 1)

    elements.foreach { element =>
      val ser = SparkEnv.get.serializer.newInstance()
      val serElement = ser.serialize(element)
      val element2 = ser.deserialize[Unstructured](serElement)
      assert(element === element2)
      assert(
        element.asInstanceOf[HtmlElement].formattedCode.get.split("\n").map(_.trim) === element2
          .asInstanceOf[HtmlElement]
          .formattedCode
          .get
          .split("\n")
          .map(_.trim)
      )
      assert(element.findAll("a").size === element2.findAll("a").size)
      assert(element.attr("class") === element2.attr("class"))
      assert(element.code === element2.code)
      assert(element.ownText === element2.ownText)
      assert(element.boilerPipe === element2.boilerPipe)
    }
  }

  ignore("Unstructured is serializable for td") {
    val elements = tablePage.findAll("table#mp-topbanner > tbody > tr > td")

    assert(elements.size === 4)

    elements.foreach { element =>
      val ser = SparkEnv.get.serializer.newInstance()
      val serElement = ser.serialize(element)
      val element2 = ser.deserialize[Unstructured](serElement)
      assert(element === element2)
      assert(
        element.asInstanceOf[HtmlElement].formattedCode.get.split("\n").map(_.trim) === element2
          .asInstanceOf[HtmlElement]
          .formattedCode
          .get
          .split("\n")
          .map(_.trim)
      )
      assert(element.findAll("a").size === element2.findAll("a").size)
      assert(element.attr("class") === element2.attr("class"))
      assert(element.code === element2.code)
      assert(element.ownText === element2.ownText)
      assert(element.boilerPipe === element2.boilerPipe)
    }
  }

  it("attrs should handles empty attributes properly") {

    assert(page.findAll("h1.central-textlogo img").attrs("title").nonEmpty)
    assert(page.findAll("h1.central-textlogo img dummy").attrs("title").isEmpty)
    assert(page.findAll("h1.central-textlogo img").attrs("dummy").isEmpty)
  }
}
