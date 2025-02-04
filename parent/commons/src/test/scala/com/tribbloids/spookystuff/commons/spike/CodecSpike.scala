package com.tribbloids.spookystuff.commons.spike

import ai.acyclic.prover.commons.debug.print_@
import com.tribbloids.spookystuff.testutils.BaseSpec

object CodecSpike {}

//@Ignore
class CodecSpike extends BaseSpec {

  trait TestCodec[T] {

    def toJson(v: T): String

    def fromJson(v: String): T

    def roundtripTest(v: T)(
        implicit
        m: Manifest[T]
    ): Unit = {

      it("" + v + ": " + m.toString()) {

        val json = toJson(v)
        print_@(json)
        val v2 = fromJson(json)
        val json2 = toJson(v2)
        assert(json == json2)
      }
    }

  }

  //  describe("encode/decode") {
  //
  //    it("Map") {
  //
  //      val v: Map[String, Int] = Map("a" -> 1, "b" -> 2)
  //      val json = toJson(v)
  //      print_@(json)
  //      val v2 = fromJson[Map[String, Int]](json)
  //      assert(v == v2)
  //    }
  //
  //    it("ListMap") {
  //
  //      val v: ListMap[String, Int] = ListMap("a" -> 1, "b" -> 2)
  //      val json = toJson(v)
  //      print_@(json)
  //      val v2 = fromJson[ListMap[String, Int]](json)
  //      assert(v == v2)
  //    }
  //
  //    it("NIO Path") {
  //
  //      import java.nio.file.Path
  //
  //      val v = Path.of("/www/google/com")
  //      val json = toJson(v)
  //      print_@(json)
  //      val v2 = fromJson[Path](json)
  //      assert(v.toString == v2.toString)
  //    }
  //
  //    it("Hadoop Path") { // this is the only one that doesn't work
  //
  //      import org.apache.hadoop.fs.Path
  //
  //      val v = new Path("/www/google/com")
  //      val json = toJson(v)
  //      print_@(json)
  //      val v2 = fromJson[Path](json)
  //      assert(v == v2)
  //    }
  //  }
}
