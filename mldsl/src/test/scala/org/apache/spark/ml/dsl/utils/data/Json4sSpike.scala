package org.apache.spark.ml.dsl.utils.data

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.json4s.{DefaultFormats, Extraction}
import org.json4s.jackson.JsonMethods
import org.scalatest.Ignore

import scala.collection.immutable.ListMap

@Ignore
class Json4sSpike extends FunSpecx {

  implicit val fm = DefaultFormats

  it("can encode/decode ListMap") {

    val v: ListMap[String, Int] = ListMap("a" -> 1, "b" -> 2)
    val json = JsonMethods.compact(Extraction.decompose(v))
    val v2 = Extraction.extract[ListMap[String, Int]](JsonMethods.parse(json))
    assert(v == v2)
  }
}
