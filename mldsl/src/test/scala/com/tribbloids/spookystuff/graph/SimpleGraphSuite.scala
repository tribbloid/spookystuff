package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.example.SimpleGraph.Face
import com.tribbloids.spookystuff.testutils.FunSpecx

class SimpleGraphSuite extends FunSpecx {

  it("DSL interface from NodeData") {

    val face = Face.fromNodeData(Some("ABC"))
    val str = face.visualise().show()
    str shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ None ]
        |+- Some(ABC)
        |   +- v (HEAD) [ None ]
        |<-- -<<
        |v (TAIL-<<) [ None ]
        |+- Some(ABC)
        |   +- v (HEAD) [ None ]
      """.stripMargin
  }

  it("... implicitly") {

    val face: Face = Some("ABC")
    val str = face.visualise().show()
    str shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ None ]
        |+- Some(ABC)
        |   +- v (HEAD) [ None ]
        |<-- -<<
        |v (TAIL-<<) [ None ]
        |+- Some(ABC)
        |   +- v (HEAD) [ None ]
      """.stripMargin

  }

  it("DSL interface from EdgeData") {

    val face = Face.fromEdgeData(Some("ABC"))
    val str = face.visualise().show()
    str shouldBe
      """
        |>>- -->
        |v (HEAD)(TAIL>>- -<<) [ Some(ABC) ]
        |<-- -<<
        |v (HEAD)(TAIL>>- -<<) [ Some(ABC) ]
      """.stripMargin

  }

  it("node >>> node") {

    val f1: Face = Some("ABC")
    val f2: Face = Some("DEF")

    val linked = f1 >>> f2
    linked.visualise().show() shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ None ]
        |+- Some(ABC)
        |   +- v  [ None ]
        |      +- Some(DEF)
        |         +- v (HEAD) [ None ]
        |<-- -<<
        |v (TAIL-<<) [ None ]
        |+- Some(DEF)
        |   +- v (HEAD) [ None ]
      """.stripMargin

    linked
      .visualise()
      .show(asciiArt = true) shouldBe
      """
        |      ┌──────────────────┐
        |      │(TAIL>>-) [ None ]│
        |      └─────────┬────────┘
        |                │
        |                v
        |           ┌─────────┐
        |           │Some(ABC)│
        |           └────┬────┘
        |                │
        |      ┌─────────┘
        |      │
        |      v
        | ┌─────────┐ ┌──────────────────┐
        | │ [ None ]│ │(TAIL-<<) [ None ]│
        | └────┬────┘ └────┬─────────────┘
        |      │           │
        |      └────────┐  │
        |               │  │
        |               v  v
        |           ┌─────────┐
        |           │Some(DEF)│
        |           └────┬────┘
        |                │
        |                v
        |        ┌───────────────┐
        |        │(HEAD) [ None ]│
        |        └───────────────┘
      """.stripMargin
  }

  it("node >>> edge >>> node") {

    val f1: Face = Some("ABC")
    val e1: Face = Face.fromEdgeData(Some("edge"))
    val f2: Face = Some("DEF")

    val linked = f1 >>> e1 >>> f2
    linked.visualise().show() shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ None ]
        |+- Some(ABC)
        |   +- v  [ Some(edge) ]
        |      +- Some(DEF)
        |         +- v (HEAD) [ None ]
        |<-- -<<
        |v (TAIL-<<) [ None ]
        |+- Some(DEF)
        |   +- v (HEAD) [ None ]
      """.stripMargin
  }

  it("detached edge >>> detached edge") {

    val f1: Face = Face.fromEdgeData(Some("ABC"))
    val f2: Face = Face.fromEdgeData(Some("DEF"))

    val linked = f1 >>> f2
    linked.visualise().show() shouldBe
      """
        |>>- -->
        |v (HEAD)(TAIL>>- -<<) [ Some(ABCDEF) ]
        |<-- -<<
        |v (HEAD)(TAIL>>- -<<) [ Some(ABCDEF) ]
      """.stripMargin
  }

  it("node >>> itself => cyclic graph") {

    val node: Face = Some("ABC")

    val n1 = node >>> node
    n1.visualise().show() shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ None ]
        |+- Some(ABC)
        |   :- v  [ None ]
        |   :  +- (cyclic)Some(ABC)
        |   +- v (HEAD) [ None ]
        |<-- -<<
        |v (TAIL-<<) [ None ]
        |+- Some(ABC)
        |   :- v  [ None ]
        |   :  +- (cyclic)Some(ABC)
        |   +- v (HEAD) [ None ]
        |
      """.stripMargin
  }

  it("node >>> edge >>> same node => cyclic graph") {

    val node: Face = Some("ABC")
    val edge: Face = Face.fromEdgeData(Some("loop"))

    val n1 = node >>> edge
    n1.visualise().show() shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ None ]
        |+- Some(ABC)
        |   +- v (HEAD) [ Some(loop) ]
        |<-- -<<
        |v (TAIL-<<) [ Some(loop) ]
      """.stripMargin

    val loop = n1 >>> node
    loop.visualise().show() shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ None ]
        |+- Some(ABC)
        |   :- v  [ Some(loop) ]
        |   :  +- (cyclic)Some(ABC)
        |   +- v (HEAD) [ None ]
        |<-- -<<
        |v (TAIL-<<) [ None ]
        |+- Some(ABC)
        |   :- v  [ Some(loop) ]
        |   :  +- (cyclic)Some(ABC)
        |   +- v (HEAD) [ None ]
      """.stripMargin
  }

  it("node >>> node <<< node") {

    val n1: Face = Some("A")
    val n2: Face = Some("B")
    val n3: Face = Some("C")

    (n1 >>> n2 <<< n3).visualise().show() shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ None ]
        |+- Some(A)
        |   +- v  [ None ]
        |      +- Some(B)
        |         +- v (HEAD) [ None ]
        |<-- -<<
        |v (TAIL-<<) [ None ]
        |+- Some(C)
        |   +- v  [ None ]
        |      +- Some(B)
        |         +- v (HEAD) [ None ]
      """.stripMargin
  }
}
