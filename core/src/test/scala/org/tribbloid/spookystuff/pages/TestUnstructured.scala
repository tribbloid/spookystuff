package org.tribbloid.spookystuff.pages

import org.apache.spark.SparkEnv
import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl

/**
 * Created by peng on 11/30/14.
 */
class TestUnstructured extends SpookyEnvSuite {

  import dsl._

  lazy val page = (Wget("http://www.wikipedia.org/").as('old)::Nil).resolve(spooky).head.asInstanceOf[Page]

  test("Unstructured is serializable for div") {
    val elements = page.children("div.central-featured-lang")

    assert(elements.size === 10)

    elements.foreach{
      element =>
        val ser = SparkEnv.get.serializer.newInstance()
        val serElement = ser.serialize(element)
        val element2 = ser.deserialize[Unstructured](serElement)
        assert (element === element2)
        assert(element.asInstanceOf[HtmlElement].parsed.outerHtml().split("\n").map(_.trim) === element2.asInstanceOf[HtmlElement].parsed.outerHtml().split("\n").map(_.trim))
        assert(element.children("a").size === element2.children("a").size)
        assert(element.attr("class") === element2.attr("class"))
        assert(element.code === element2.code)
        assert(element.ownText === element2.ownText)
        assert(element.boilerPipe === element2.boilerPipe)
    }
  }

  lazy val tablePage = (Wget("http://en.wikipedia.org/").as('old)::Nil).resolve(spooky).head.asInstanceOf[Page]

  test("Unstructured is serializable for tr") {
    val elements = tablePage.children("table#mp-topbanner > tbody > tr")

    assert(elements.size === 1)

    elements.foreach{
      element =>
        val ser = SparkEnv.get.serializer.newInstance()
        val serElement = ser.serialize(element)
        val element2 = ser.deserialize[Unstructured](serElement)
        assert (element === element2)
        assert(element.asInstanceOf[HtmlElement].parsed.outerHtml().split("\n").map(_.trim) === element2.asInstanceOf[HtmlElement].parsed.outerHtml().split("\n").map(_.trim))
        assert(element.children("a").size === element2.children("a").size)
        assert(element.attr("class") === element2.attr("class"))
        assert(element.code === element2.code)
        assert(element.ownText === element2.ownText)
        assert(element.boilerPipe === element2.boilerPipe)
    }
  }

  test("Unstructured is serializable for td") {
    val elements = tablePage.children("table#mp-topbanner > tbody > tr > td")

    assert(elements.size === 4)

    elements.foreach{
      element =>
        val ser = SparkEnv.get.serializer.newInstance()
        val serElement = ser.serialize(element)
        val element2 = ser.deserialize[Unstructured](serElement)
        assert (element === element2)
        assert(element.asInstanceOf[HtmlElement].parsed.outerHtml().split("\n").map(_.trim) === element2.asInstanceOf[HtmlElement].parsed.outerHtml().split("\n").map(_.trim))
        assert(element.children("a").size === element2.children("a").size)
        assert(element.attr("class") === element2.attr("class"))
        assert(element.code === element2.code)
        assert(element.ownText === element2.ownText)
        assert(element.boilerPipe === element2.boilerPipe)
    }
  }

  test("attrs should handles empty attributes properly") {

    assert(page.children("h1.central-textlogo img").attrs("title").nonEmpty)
    assert(page.children("h1.central-textlogo img dummy").attrs("title").isEmpty)
    assert(page.children("h1.central-textlogo img").attrs("dummy").isEmpty)
  }
}
