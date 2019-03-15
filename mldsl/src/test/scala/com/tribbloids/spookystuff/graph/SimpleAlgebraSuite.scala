package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.example.Simple
import com.tribbloids.spookystuff.testutils.FunSpecx

class SimpleAlgebraSuite extends FunSpecx {

  it("create DSL core from NodeData") {

    val core = Simple.Face.fromNodeData(Some("Core1"))
    val str = core.visualise().show()
    str.shouldBe(
      """
        |->>
        |v (TAIL->> <<-) [ None ]
        |+- Some(Core1)
        |<<-
        |v (TAIL->> <<-) [ None ]
        |+- Some(Core1)
      """.stripMargin
    )
  }

  it("implicitly") {

    val core: Simple.Face = Some("Core1")
    val str = core.visualise().show()
    str.shouldBe(
      """
        |->>
        |v (TAIL->> <<-) [ None ]
        |+- Some(Core1)
        |<<-
        |v (TAIL->> <<-) [ None ]
        |+- Some(Core1)
      """.stripMargin
    )
  }

  it("create DSL core from EdgeData") {

    val core = Simple.Face.fromEdgeData(Some("Core1"))
    val str = core.visualise().show()
    str.shouldBe(
      """
        |->>
        |v (HEAD)(TAIL->> <<-) [ Some(Core1) ]
        |<<-
        |v (HEAD)(TAIL->> <<-) [ Some(Core1) ]
      """.stripMargin
    )
  }

}
