package org.apache.spark.ml.dsl.utils.data

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.dsl.utils.data.EAV.Impl

class EAVSuite extends FunSpecx {

  val wellformed = Impl(
    "int" -> 1,
    "double" -> 2.5,
    "itr" -> Seq("a", "b"),
    "map" -> Map("a" -> 1, "b" -> 2)
  )

  val malformed = wellformed :++ Impl(
    "path" -> new Path("file://home/dir")
  )
  val malformedFixed = Impl(self = malformed.self.updated("path", "file://home/dir"))

  val nested = malformed :++ Impl(
    "children" -> malformed.self
  )
  val nestedFixed = malformedFixed :++ Impl(
    "children" -> malformedFixed.self
  )

  val withNull = wellformed :++ Impl("nullable" -> null)

  it("wellformed <=> JSON") {
    val o = wellformed
    val json = o.toJSON()
    json.shouldBe(
      """
        |{
        |  "int" : 1,
        |  "double" : 2.5,
        |  "itr" : [ "a", "b" ],
        |  "map" : {
        |    "a" : 1,
        |    "b" : 2
        |  }
        |}
      """.stripMargin
    )
    val back: Impl = Impl.fromJSON(json)
    assert(o == back)
  }

  it("malformed <=> JSON") {
    val o = malformed
    val json = o.toJSON()
    json.jsonShouldBe(
      """
        |{
        |  "int" : 1,
        |  "double" : 2.5,
        |  "itr" : [ "a", "b" ],
        |  "map" : {
        |    "a" : 1,
        |    "b" : 2
        |  },
        |  "path" : "file://home/dir"
        |}
      """.stripMargin
    )
    val back: Impl = Impl.fromJSON(json)
    assert(malformedFixed == back)
  }

  it("nested <=> JSON") {
    val o = nested
    val json = o.toJSON()
    json.jsonShouldBe(
      """
        |{
        |  "int" : 1,
        |  "double" : 2.5,
        |  "itr" : [ "a", "b" ],
        |  "map" : {
        |    "a" : 1,
        |    "b" : 2
        |  },
        |  "path" : "file://home/dir",
        |  "children" : {
        |    "int" : 1,
        |    "double" : 2.5,
        |    "itr" : [ "a", "b" ],
        |    "map" : {
        |      "a" : 1,
        |      "b" : 2
        |    },
        |    "path" : "file://home/dir"
        |  }
        |}
      """.stripMargin
    )
    val back: Impl = Impl.fromJSON(json)
    assert(nestedFixed == back)
  }

  it("withNull <=> JSON") {

    val o = withNull
    val json = o.toJSON()
    json.shouldBe(
      """
        |{
        |  "nullable" : null,
        |  "itr" : [ "a", "b" ],
        |  "map" : {
        |    "a" : 1,
        |    "b" : 2
        |  },
        |  "double" : 2.5,
        |  "int" : 1
        |}
      """.stripMargin
    )
    val back: Impl = Impl.fromJSON(json)
    assert(withNull == back)
  }
}
