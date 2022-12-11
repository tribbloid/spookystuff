package org.apache.spark.ml.dsl.utils.data

import com.tribbloids.spookystuff.testutils.FunSpecx

class EAVSuite extends FunSpecx {

  import EAVSuite._
  import EAVSystem.NoAttr._

  val wellformed = EAVSystem.NoAttr.From(
    "int" -> 1,
    "double" -> 2.5,
    "itr" -> Seq("a", "b"),
    "map" -> EAVSystem.NoAttr.From("a" -> 1, "b" -> 2),
    "path" -> "file://home/dir"
  )

  val nested = wellformed :++ EAVSystem.NoAttr.From(
    "children" -> wellformed
  )

  val withNull = wellformed :++ EAVSystem.NoAttr.From("nullable" -> null)

  val withEnum = WithEnum._EAV(wellformed.updated("enumField" -> "bb").internal)

  it("wellformed <=> JSON") {

    val o: _EAV = wellformed
    val json = o.toJSON()
    json.shouldBe(
      """
        |{
        |  "double" : 2.5,
        |  "int" : 1,
        |  "itr" : [ "a", "b" ],
        |  "map" : {
        |    "a" : 1,
        |    "b" : 2
        |  },
        |  "path" : "file://home/dir"
        |}
      """.stripMargin
    )
    val back = EAVSystem.NoAttr.relay.fromJSON(json)
    assert(back == o)
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
    val back = EAVSystem.NoAttr.relay.fromJSON(json)
    assert(back == nested)
  }

  it("withNull <=> JSON") {

    val o = withNull
    val json = o.toJSON()
    json.jsonShouldBe(
      """
        |{
        |  "nullable" : null,
        |  "itr" : [ "a", "b" ],
        |  "map" : {
        |    "a" : 1,
        |    "b" : 2
        |  },
        |  "double" : 2.5,
        |  "path" : "file://home/dir",
        |  "int" : 1
        |}
      """.stripMargin
    )
    val back = EAVSystem.NoAttr.relay.fromJSON(json)
    assert(back == withNull)
  }

  it("tryGetEnum can convert String to Enumeration") {

    val v = withEnum.enumField.tryGetEnum(EAVSuite.EE)
    assert(v.get == EAVSuite.EE.bb)
  }
}

object EAVSuite {

  object WithEnum extends EAVSystem {

    case class _EAV(internal: collection.Map[String, Any]) extends ThisEAV {
      override type Bound = Any

      object enumField extends Attr[String]()
    }
  }

  object EE extends Enumeration {

    val aa, bb, cc, dd = Value
  }
}
