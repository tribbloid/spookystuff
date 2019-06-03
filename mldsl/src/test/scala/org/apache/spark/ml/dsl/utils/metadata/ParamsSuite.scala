package org.apache.spark.ml.dsl.utils.metadata

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.hadoop.fs.Path

class ParamsSuite extends FunSpecx {

  val wellformed = Params(
    "int" -> 1,
    "double" -> 2.5,
    "itr" -> Seq("a", "b"),
    "map" -> Map("a" -> 1, "b" -> 2)
  )

  val malformed = new Params(
    wellformed.self ++ Map(
      "path" -> new Path("file://home/dir/")
    )
  )
  val malformedFixed = new Params(self = malformed.self.updated("path", "file://home/dir/"))

  val nested = new Params(
    malformed.self ++ Map(
      "children" -> malformed.self
    )
  )
  val nestedFixed = new Params(
    malformedFixed.self ++ Map(
      "children" -> malformedFixed.self
    )
  )

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
    val back: Params = Params.fromJSON(json)
    assert(o == back)
  }

  //TODO: report bug and fix
  ignore("malformed <=> JSON") {
    val o = malformed
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
        |  },
        |  "path" : "file://home/dir"
        |}
      """.stripMargin
    )
    val back: Params = Params.fromJSON(json)
    assert(malformedFixed == back)
  }

  //TODO: report bug and fix
  ignore("nested <=> JSON") {
    val o = nested
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
    val back: Params = Params.fromJSON(json)
    assert(nestedFixed == back)
  }
}
