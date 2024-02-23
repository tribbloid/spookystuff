package com.tribbloids.spookystuff.commons.data

import com.tribbloids.spookystuff.testutils.BaseSpec
import org.apache.hadoop.fs.Path
import org.json4s.{DefaultFormats, Extraction}
import org.json4s.jackson.JsonMethods
import org.scalatest.Ignore

import scala.collection.immutable.ListMap

@Ignore
class Json4sSpike extends BaseSpec {

  implicit val fm: DefaultFormats.type = DefaultFormats

  describe("encode/decode") {

    it("ListMap") {

      val v: ListMap[String, Int] = ListMap("a" -> 1, "b" -> 2)
      val json = JsonMethods.compact(Extraction.decompose(v))
      val v2 = Extraction.extract[ListMap[String, Int]](JsonMethods.parse(json))
      assert(v == v2)
    }

    it("Path") {

      val v = new Path("http://www.google.com")
      val json = JsonMethods.compact(Extraction.decompose(v))
      val v2 = Extraction.extract[Path](JsonMethods.parse(json))
      assert(v == v2)
    }
  }

}
