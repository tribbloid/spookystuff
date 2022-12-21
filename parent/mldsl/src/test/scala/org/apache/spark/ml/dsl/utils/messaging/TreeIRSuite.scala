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

      val relay = TreeIR._Relay
      val back = relay.fromJSON(jv)

      val jv2 = back.toJSON()

      jv2.shouldBe(jv)

      back.toString shouldBe o3.toString

      back.treeView.treeString.shouldBe(
        """
          |StructTree
          |:- StructTree
          |:  :- Leaf 9223372036854775807
          |:  +- StructTree
          |:     :- Leaf 1
          |:     +- Leaf 2.0
          |+- Leaf FF
          |""".stripMargin
      )
    }
  }
}

object TreeIRSuite {

  val o1 = TreeIR.struct("a" -> TreeIR.leaf(1), "b" -> TreeIR.leaf(2.0))

  val o2 = TreeIR.struct("c" -> TreeIR.leaf(Long.MaxValue), "d" -> o1)

  val o3 = TreeIR.struct("e" -> o2, "f" -> TreeIR.leaf("FF"))
}
