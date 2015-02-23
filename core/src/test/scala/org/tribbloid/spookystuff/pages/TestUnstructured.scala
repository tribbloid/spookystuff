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

  lazy val page = Trace(Wget("http://www.wikipedia.org/").as('old)::Nil).resolve(spooky).head.asInstanceOf[Page]

  test("Unstructured is serializable") {
    val elements = page.children("div.central-featured-lang")

    assert(elements.size === 10)

    elements.foreach{
      element =>
        val ser = SparkEnv.get.serializer.newInstance()
        val serElement = ser.serialize(element)
        val element2 = ser.deserialize[Unstructured](serElement)
        assert (element === element2)
    }
  }

  test("attrs") {

    assert(page.children("h1.central-textlogo img").attrs("title").nonEmpty)
    assert(page.children("h1.central-textlogo img dummy").attrs("title").isEmpty)
    assert(page.children("h1.central-textlogo img").attrs("dummy").isEmpty)
  }
}
