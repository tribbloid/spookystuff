package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.example.SimpleGraph.DSL
import com.tribbloids.spookystuff.graph.example.SimpleGraph.DSL._
import com.tribbloids.spookystuff.testutils.FunSpecx

class SimpleGraphSuite extends FunSpecx {

  it("DSL interface from NodeData") {

    val face = Node(Some("ABC"))
    val str = face.visualise().show()
    str shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ ∅ ]
        |+- ABC
        |   +- v (HEAD) [ ∅ ]
        |<-- -<<
        |v (TAIL-<<) [ ∅ ]
        |+- ABC
        |   +- v (HEAD) [ ∅ ]
      """.stripMargin
  }

  it("... implicitly") {

    val face = Some("ABC")
    val str = face.visualise().show()
    str shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ ∅ ]
        |+- ABC
        |   +- v (HEAD) [ ∅ ]
        |<-- -<<
        |v (TAIL-<<) [ ∅ ]
        |+- ABC
        |   +- v (HEAD) [ ∅ ]
      """.stripMargin

  }

  it("DSL interface from EdgeData") {

    val face = Edge(Some("ABC"))
    val str = face.visualise().show()
    str shouldBe
      """
        |>>- -->
        |v (HEAD)(TAIL>>- -<<) [ ABC ]
        |<-- -<<
        |v (HEAD)(TAIL>>- -<<) [ ABC ]
      """.stripMargin

  }

  it("node >>> node") {

    val f1: DSL = Some("ABC")
    val f2: DSL = Some("DEF")

    val linked = f1 >>> f2
    linked.visualise().show() shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ ∅ ]
        |+- ABC
        |   +- v  [ ∅ ]
        |      +- DEF
        |         +- v (HEAD) [ ∅ ]
        |<-- -<<
        |v (TAIL-<<) [ ∅ ]
        |+- DEF
        |   +- v (HEAD) [ ∅ ]
      """.stripMargin

    linked
      .visualise()
      .show(asciiArt = true) shouldBe
      """
        |     ┌───────────────┐
        |     │(TAIL>>-) [ ∅ ]│
        |     └───────┬───────┘
        |             │
        |             v
        |           ┌───┐
        |           │ABC│
        |           └─┬─┘
        |             │
        |     ┌───────┘
        |     │
        |     v
        | ┌──────┐ ┌───────────────┐
        | │ [ ∅ ]│ │(TAIL-<<) [ ∅ ]│
        | └───┬──┘ └───┬───────────┘
        |     │        │
        |     └──────┐ │
        |            │ │
        |            v v
        |          ┌─────┐
        |          │ DEF │
        |          └──┬──┘
        |             │
        |             v
        |      ┌────────────┐
        |      │(HEAD) [ ∅ ]│
        |      └────────────┘
      """.stripMargin
  }

  it("node >>> edge >>> node") {

    val f1: DSL = Some("ABC")
    val e1: DSL = Edge(Some("edge"))
    val f2: DSL = Some("DEF")

    val linked = f1 >>> e1 >>> f2
    linked.visualise().show() shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ ∅ ]
        |+- ABC
        |   +- v  [ edge ]
        |      +- DEF
        |         +- v (HEAD) [ ∅ ]
        |<-- -<<
        |v (TAIL-<<) [ ∅ ]
        |+- DEF
        |   +- v (HEAD) [ ∅ ]
      """.stripMargin
  }

  it("detached edge >>> detached edge") {

    val f1: DSL = Edge(Some("ABC"))
    val f2: DSL = Edge(Some("DEF"))

    val linked = f1 >>> f2
    linked.visualise().show() shouldBe
      """
        |>>- -->
        |v (HEAD)(TAIL>>- -<<) [ ABCDEF ]
        |<-- -<<
        |v (HEAD)(TAIL>>- -<<) [ ABCDEF ]
      """.stripMargin
  }

  describe("=> cyclic graph") {

    it("node >>> itself") {

      val node: DSL = Some("ABC")

      val linked = node >>> node
      linked.visualise().show() shouldBe
        """
          |>>- -->
          |v (TAIL>>-) [ ∅ ]
          |+- ABC
          |   :- v  [ ∅ ]
          |   :  +- (cyclic)ABC
          |   +- v (HEAD) [ ∅ ]
          |<-- -<<
          |v (TAIL-<<) [ ∅ ]
          |+- ABC
          |   :- v  [ ∅ ]
          |   :  +- (cyclic)ABC
          |   +- v (HEAD) [ ∅ ]
        """.stripMargin

      linked.visualise().show(asciiArt = true) shouldBe
        """
          | ┌───────────────┐ ┌───────────────┐
          | │(TAIL-<<) [ ∅ ]│ │(TAIL>>-) [ ∅ ]│
          | └──────────────┬┘ └───────┬───────┘
          |                │          │
          |                │  ┌───────┘
          |                │  │
          |                v  v
          |             ┌───────┐
          |             │  ABC  │
          |             └─┬───┬─┘
          |               │ ^ │
          |       ┌───────┘ │ │
          |       │ ┌───────┘ │
          |       v │         v
          |    ┌────┴─┐    ┌────────────┐
          |    │ [ ∅ ]│    │(HEAD) [ ∅ ]│
          |    └──────┘    └────────────┘
        """.stripMargin
    }

    it("node >>> edge >>> same node") {

      val node: DSL = Some("ABC")
      val edge: DSL = Edge(Some("loop"))

      val n1 = node >>> edge
      n1.visualise().show() shouldBe
        """
          |>>- -->
          |v (TAIL>>-) [ ∅ ]
          |+- ABC
          |   +- v (HEAD) [ loop ]
          |<-- -<<
          |v (TAIL-<<) [ loop ]
        """.stripMargin

      val linked = n1 >>> node
      linked.visualise().show() shouldBe
        """
          |>>- -->
          |v (TAIL>>-) [ ∅ ]
          |+- ABC
          |   :- v (HEAD) [ ∅ ]
          |   +- v  [ loop ]
          |      +- (cyclic)ABC
          |<-- -<<
          |v (TAIL-<<) [ ∅ ]
          |+- ABC
          |   :- v (HEAD) [ ∅ ]
          |   +- v  [ loop ]
          |      +- (cyclic)ABC
        """.stripMargin

      linked.visualise().show(asciiArt = true) shouldBe
        """
          | ┌───────────────┐ ┌───────────────┐
          | │(TAIL-<<) [ ∅ ]│ │(TAIL>>-) [ ∅ ]│
          | └──────────────┬┘ └───────┬───────┘
          |                │          │
          |                │  ┌───────┘
          |                │  │
          |                v  v
          |             ┌───────┐
          |             │  ABC  │
          |             └─┬───┬─┘
          |               │ ^ │
          |       ┌───────┘ │ │
          |       │  ┌──────┘ │
          |       v  │        v
          |   ┌──────┴──┐   ┌────────────┐
          |   │ [ loop ]│   │(HEAD) [ ∅ ]│
          |   └─────────┘   └────────────┘
        """.stripMargin
    }

    it("edge-node >>> itself") {

      val edge_node: DSL = Edge(Some("loop")) >>> Node(Some("ABC"))

      val linked = edge_node >>> edge_node
      linked.visualise().show(asciiArt = true) shouldBe
        """
          | ┌──────────────────┐ ┌───────────────┐
          | │(TAIL>>-) [ loop ]│ │(TAIL-<<) [ ∅ ]│
          | └────────────────┬─┘ └───────┬───────┘
          |                  │           │
          |                  │  ┌────────┘
          |                  │  │
          |                  v  v
          |               ┌───────┐
          |               │  ABC  │
          |               └─┬───┬─┘
          |                 │ ^ │
          |        ┌────────┘ │ │
          |        │  ┌───────┘ │
          |        v  │         v
          |    ┌──────┴──┐    ┌────────────┐
          |    │ [ loop ]│    │(HEAD) [ ∅ ]│
          |    └─────────┘    └────────────┘
        """.stripMargin
    }

    it("(2 edges >- node ) >>> itself") {

      val edges_node = (Edge(Some("loop1")) U Edge(Some("loop2"))) >>> Node(Some("ABC"))

      val linked = edges_node >>> edges_node
      linked.visualise().show(asciiArt = true) shouldBe
        """
          | ┌───────────────────┐ ┌───────────────────┐ ┌───────────────┐
          | │(TAIL>>-) [ loop2 ]│ │(TAIL>>-) [ loop1 ]│ │(TAIL-<<) [ ∅ ]│
          | └─────────┬─────────┘ └──────┬────────────┘ └───────┬───────┘
          |           │                  │                      │
          |           └───────────────┐  │   ┌──────────────────┘
          |                           │  │   │
          |                           v  v   v
          |                        ┌───────────┐
          |                        │    ABC    │
          |                        └─┬┬──────┬─┘
          |                          ││^  ^  │
          |         ┌────────────────┘││  │  └───────────┐
          |         │   ┌─────────────┼┘  │              │
          |         v   │             v   │              v
          |     ┌───────┴──┐     ┌────────┴─┐     ┌────────────┐
          |     │ [ loop2 ]│     │ [ loop1 ]│     │(HEAD) [ ∅ ]│
          |     └──────────┘     └──────────┘     └────────────┘
        """.stripMargin
    }
  }

  it("node >>> node <<< node") {

    val n1: DSL = Some("A")
    val n2: DSL = Some("B")
    val n3: DSL = Some("C")

    (n1 >>> n2 <<< n3).visualise().show() shouldBe
      """
        |>>- -->
        |v (TAIL>>-) [ ∅ ]
        |+- A
        |   +- v  [ ∅ ]
        |      +- B
        |         +- v (HEAD) [ ∅ ]
        |<-- -<<
        |v (TAIL-<<) [ ∅ ]
        |+- C
        |   +- v  [ ∅ ]
        |      +- B
        |         +- v (HEAD) [ ∅ ]
      """.stripMargin
  }
}
