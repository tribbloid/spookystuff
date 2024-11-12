package com.tribbloids.spookystuff.commons.data

import com.tribbloids.spookystuff.commons.data.EAVSystem
import com.tribbloids.spookystuff.testutils.BaseSpec
import com.tribbloids.spookystuff.relay.Relay

class EAVLikeSuite extends BaseSpec {

  import EAVLikeSuite.*
  import EAVSystem.NoAttr.*

  val wellformed: ^ = EAVSystem.NoAttr.From(
    "int" -> 1,
    "double" -> 2.5,
    "itr" -> Seq("a", "b"),
    "map" -> EAVSystem.NoAttr.From("a" -> 1, "b" -> 2),
    "path" -> "file://home/dir"
  )

  val nested: ^ = wellformed :++ EAVSystem.NoAttr.From(
    "children" -> wellformed
  )

  val withNull: ^ = wellformed :++ EAVSystem.NoAttr.From("nullable" -> null)

  val withEnum: WithEnum.^ = WithEnum.^(wellformed.updated("enumField" -> "bb").internal)

  val noAttrDecoderView: EAVSystem.NoAttr._Relay#DecoderView = Relay.toFallbackDecoderView(EAVSystem.NoAttr.relay)

  it("wellformed <=> JSON") {

    val o: ^ = wellformed
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

  object WithEnum extends EAVSystem {

    case class ^(internal: collection.Map[String, Any]) extends EAV {

      case object enumField extends Attr[String]()
    }
  }

  object EE extends Enumeration {

    val aa, bb, cc, dd = Value
  }
}
