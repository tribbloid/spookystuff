package org.tribbloid.spookystuff.pages

import org.apache.spark.SparkEnv
import org.tribbloid.spookystuff.{dsl, SparkEnvSuite}
import org.tribbloid.spookystuff.actions.{Trace, Wget}
import dsl._

/**
 * Created by peng on 11/30/14.
 */
class TestUnstructured extends SparkEnvSuite {

  val page = Trace(Wget("http://www.wikipedia.org/").as('old)::Nil).resolve(spooky)

  test("Unstructured is serializable") {
    val elements = page.allChildren("div.central-featured-lang")

    assert(elements.size === 10)

    elements.foreach{
      element =>
        val ser = SparkEnv.get.serializer.newInstance()
        val serElement = ser.serialize(element)
        val element2 = ser.deserialize[Unstructured](serElement)
        assert (element === element2)
    }
  }
}
