package org.apache.spark.ml.dsl.utils.messaging

import com.tribbloids.spookystuff.testutils.FunSpecx

class TreeIRSuite extends FunSpecx {

  import TreeIRSuite._

  describe("from/to JSON") {

    it("round-trip") {

      val jv = o3.toJSON()
      jv.shouldBe(
        """
          |{
          |  "e" : {
          |    "c" : 9223372036854775807,
          |    "d" : {
          |      "a" : 1,
          |      "b" : 2.0
          |    }
          |  },
          |  "f" : "FF"
          |}
          |""".stripMargin
      )

      val reader = TreeIR.Codec[Any]()
      val back = reader.fromJSON(jv)

      val jv2 = back.toJSON()

      jv2.shouldBe(jv)

      back.toString shouldBe o3.toString

      back.treeView.treeString.shouldBe(
        """
          |IRObject
          |:- IRObject
          |:  :- IRValue 9223372036854775807
          |:  +- IRObject
          |:     :- IRValue 1
          |:     +- IRValue 2.0
          |+- IRValue FF
          |""".stripMargin
      )
    }
  }
}

object TreeIRSuite {

  val o1 = TreeIR.fromKVs("a" -> 1, "b" -> 2.0)

  val o2 = TreeIR.fromKVs[Any]("c" -> Long.MaxValue, "d" -> o1)

  val o3 = TreeIR.fromKVs[Any]("e" -> o2, "f" -> "FF")
}
