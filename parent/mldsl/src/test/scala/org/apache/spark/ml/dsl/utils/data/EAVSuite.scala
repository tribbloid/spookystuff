package org.apache.spark.ml.dsl.utils.data

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.dsl.utils.data.EAV.Impl

import scala.reflect.ClassTag

class EAVSuite extends FunSpecx {

  val wellformed = Impl(
    "int" -> 1,
    "double" -> 2.5,
    "itr" -> Seq("a", "b"),
    "map" -> Impl("a" -> 1, "b" -> 2),
    "path" -> "file://home/dir"
  )

  val nested = wellformed :++ Impl(
    "children" -> wellformed
  )

  val withNull = wellformed :++ Impl("nullable" -> null)

  case class WithEnum(source: EAV) extends EAV {
    override type VV = Any
    override def ctg: ClassTag[Any] = getCtg

    object enumField extends Attr[String]()
  }

  val withEnum: WithEnum = WithEnum(wellformed.updated("enumField" -> "bb"))

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
        |  },
        |  "path" : "file://home/dir"
        |}
      """.stripMargin
    )
    val back: Impl = Impl.fromJSON(json)
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
    val back: Impl = Impl.fromJSON(json)
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
    val back: Impl = Impl.fromJSON(json)
    assert(back == withNull)
  }

  it("tryGetEnum can convert String to Enumeration") {

    val v = withEnum.enumField.tryGetEnum(EAVSuite.EE)
    assert(v.get == EAVSuite.EE.bb)
  }
}

object EAVSuite {

  object EE extends Enumeration {

    val aa, bb, cc, dd = Value
  }
}
