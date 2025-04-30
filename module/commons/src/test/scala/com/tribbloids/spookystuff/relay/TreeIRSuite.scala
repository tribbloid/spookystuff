package com.tribbloids.spookystuff.relay

import com.tribbloids.spookystuff.testutils.BaseSpec

class TreeIRSuite extends BaseSpec {

  import TreeIRSuite.*

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

      val relay = TreeIR._Relay
      val back = relay.fromJSON(jv)

      val jv2 = back.toJSON()

      jv2.shouldBe(jv)

      back.toString shouldBe o3.toString

      back.treeView.treeString.shouldBe(
        """
          |+ Map
          |!-+ Map
          |: !-- BigInt 9223372036854775807
          |: !-+ Map
          |:   !-- BigInt 1
          |:   !-- Double 2.0
          |!-- String FF
          |""".stripMargin
      )
    }
  }
}

object TreeIRSuite {

  val o1 = TreeIR.map("a" -> TreeIR.leaf(1), "b" -> TreeIR.leaf(2.0))

  val o2 = TreeIR.map("c" -> TreeIR.leaf(Long.MaxValue), "d" -> o1)

  val o3 = TreeIR.map("e" -> o2, "f" -> TreeIR.leaf("FF"))
}
