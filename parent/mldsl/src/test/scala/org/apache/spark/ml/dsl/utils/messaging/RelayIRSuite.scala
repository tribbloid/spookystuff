package org.apache.spark.ml.dsl.utils.messaging

import com.tribbloids.spookystuff.testutils.FunSpecx

class RelayIRSuite extends FunSpecx {

  import RelayIRSuite._

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

      val reader = RelayIR.Codec[Any]()
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

object RelayIRSuite {

  val o1 = RelayIR.buildFromKVs("a" -> 1, "b" -> 2.0)

  val o2 = RelayIR.buildFromKVs[Any]("c" -> Long.MaxValue, "d" -> o1)

  val o3 = RelayIR.buildFromKVs[Any]("e" -> o2, "f" -> "FF")
}
