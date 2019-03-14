package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.example.Simple.SimpleDSL
import com.tribbloids.spookystuff.testutils.FunSpecx

class SimpleSuite extends FunSpecx {

  it("create DSL core from NodeData") {

    val core = SimpleDSL.Core.fromNodeData(Some("Core1"))
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

    val core: SimpleDSL.Core = Some("Core1")
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

    val core = SimpleDSL.Core.fromEdgeData(Some("Core1"))
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
