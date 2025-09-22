package com.tribbloids.spookystuff.commons.data

import com.tribbloids.spookystuff.commons.data.EAVSchema
import com.tribbloids.spookystuff.testutils.BaseSpec
import com.tribbloids.spookystuff.relay.Relay

class EAVLikeSuite extends BaseSpec {

  import EAVLikeSuite.*
  import EAVSchema.NoAttr.*

  val wellformed: EAV = EAVSchema.NoAttr(
    "int" -> 1,
    "double" -> 2.5,
    "itr" -> Seq("a", "b"),
    "map" -> EAVSchema.NoAttr("a" -> 1, "b" -> 2),
    "path" -> "file://home/dir"
  )

  val nested: EAV = wellformed :++ EAVSchema.NoAttr(
    "children" -> wellformed
  )

  val withNull: EAV = wellformed :++ EAVSchema.NoAttr("nullable" -> null)

  val withEnum: WithEnum.EAV = WithEnum.EAV(wellformed.updated("enumField" -> "bb").internal)

  val noAttrDecoderView: EAVSchema.NoAttr._Relay#DecoderView = Relay.toFallbackDecoderView(EAVSchema.NoAttr.relay)

  it("wellformed <=> JSON") {

    val o: EAV = wellformed
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
    val back = noAttrDecoderView.fromJSON(json)
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
    val back = noAttrDecoderView.fromJSON(json)
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
    val back = noAttrDecoderView.fromJSON(json)
    assert(back == withNull)
  }

  it("tryGetEnum can convert String to Enumeration") {

    val v = withEnum.enumField.asEnum(EAVLikeSuite.EE).tryGet
    assert(v.get == EAVLikeSuite.EE.bb)
  }
}

object EAVLikeSuite {

  object WithEnum extends EAVSchema {

    implicit class EAV(val internal: collection.Map[String, Any]) extends EAVMixin {

      case object enumField extends Attr[String]()
    }
  }

  object EE extends Enumeration {

    val aa, bb, cc, dd = Value
  }
}
