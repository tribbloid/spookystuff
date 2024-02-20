package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.example.SimpleFlowGraph.DSL._
import com.tribbloids.spookystuff.testutils.BaseSpec

class FlowLayoutSuite extends BaseSpec {

  import Implicits._

  it("Operand from NodeData") {

    val face = Node(Some("ABC"))
    val str = face.visualise().showStr()
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
    val str = face.visualise().showStr()
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

  it("Operand from EdgeData") {

    val face = Edge(Some("ABC"))
    val str = face.visualise().showStr()
    str shouldBe
      """
        |>>- -->
        |v (HEAD)(TAIL>>- -<<) [ ABC ]
        |<-- -<<
        |v (HEAD)(TAIL>>- -<<) [ ABC ]
      """.stripMargin

  }

  describe("acyclic") {

    it("node >>> node") {

      val f1: Op = Some("ABC")
      val f2: Op = Some("DEF")

      val linked = f1 :>> f2
      linked.visualise().showStr() shouldBe
        """
          |>>- -->
          |v (TAIL>>-) [ ∅ ]
          |+- ABC
          |   +- v [ ∅ ]
          |      +- DEF
          |         +- v (HEAD) [ ∅ ]
          |<-- -<<
          |v (TAIL-<<) [ ∅ ]
          |+- DEF
          |   +- v (HEAD) [ ∅ ]
      """.stripMargin

      linked
        .visualise()
        .showStr(asciiArt = true) shouldBe
        """
          |    ┌───────────────┐
          |    │(TAIL>>-) [ ∅ ]│
          |    └───────┬───────┘
          |            │
          |            v
          |          ┌───┐
          |          │ABC│
          |          └─┬─┘
          |            │
          |    ┌───────┘
          |    │
          |    v
          | ┌─────┐ ┌───────────────┐
          | │[ ∅ ]│ │(TAIL-<<) [ ∅ ]│
          | └──┬──┘ └───┬───────────┘
          |    │        │
          |    └──────┐ │
          |           │ │
          |           v v
          |         ┌─────┐
          |         │ DEF │
          |         └───┬─┘
          |             │
          |             v
          |      ┌────────────┐
          |      │(HEAD) [ ∅ ]│
          |      └────────────┘
      """.stripMargin
    }

    it("node >>> edge >>> node") {

      val f1: Op = Some("ABC")
      val e1: Op = Edge(Some("edge"))
      val f2: Op = Some("DEF")

      val linked = f1 :>> e1 :>> f2
      linked.visualise().showStr() shouldBe
        """
          |>>- -->
          |v (TAIL>>-) [ ∅ ]
          |+- ABC
          |   +- v [ edge ]
          |      +- DEF
          |         +- v (HEAD) [ ∅ ]
          |<-- -<<
          |v (TAIL-<<) [ ∅ ]
          |+- DEF
          |   +- v (HEAD) [ ∅ ]
      """.stripMargin
    }

    it("detached edge >>> detached edge") {

      val f1: Op = Edge(Some("ABC"))
      val f2: Op = Edge(Some("DEF"))

      val linked = f1 :>> f2
      linked.visualise().showStr() shouldBe
        """
          |>>- -->
          |v (HEAD)(TAIL>>- -<<) [ ABCDEF ]
          |<-- -<<
          |v (HEAD)(TAIL>>- -<<) [ ABCDEF ]
      """.stripMargin
    }
  }

  describe("cyclic") {

    it("node >>> itself") {

      val node: Op = Some("ABC")

      val linked = node :>> node
      linked.visualise().showStr() shouldBe
        """
          |>>- -->
          |v (TAIL>>-) [ ∅ ]
          |+- ABC
          |   :- v (HEAD) [ ∅ ]
          |   +- v [ ∅ ]
          |      +- (cyclic)ABC
          |<-- -<<
          |v (TAIL-<<) [ ∅ ]
          |+- ABC
          |   :- v (HEAD) [ ∅ ]
          |   +- v [ ∅ ]
          |      +- (cyclic)ABC
        """.stripMargin

      linked.visualise().showStr(asciiArt = true) shouldBe
        """
          | ┌───────────────┐ ┌───────────────┐
          | │(TAIL>>-) [ ∅ ]│ │(TAIL-<<) [ ∅ ]│
          | └──────────────┬┘ └───────┬───────┘
          |                │          │
          |                │  ┌───────┘
          |                │  │
          |                v  v
          |             ┌───────┐
          |             │  ABC  │
          |             └─┬─┬───┘
          |               │ │ ^
          |               │ │ └──────┐
          |               │ └──────┐ │
          |               │        │ │
          |               v        v │
          |    ┌────────────┐    ┌───┴─┐
          |    │(HEAD) [ ∅ ]│    │[ ∅ ]│
          |    └────────────┘    └─────┘
        """.stripMargin
    }

    it("node >>> edge >>> same node") {

      val node: Op = Some("ABC")
      val edge: Op = Edge(Some("loop"))

      val n1 = node :>> edge
      n1.visualise().showStr() shouldBe
        """
          |>>- -->
          |v (TAIL>>-) [ ∅ ]
          |+- ABC
          |   +- v (HEAD) [ loop ]
          |<-- -<<
          |v (TAIL-<<) [ loop ]
        """.stripMargin

      val linked = n1 :>> node
      linked.visualise().showStr() shouldBe
        """
          |>>- -->
          |v (TAIL>>-) [ ∅ ]
          |+- ABC
          |   :- v (HEAD) [ ∅ ]
          |   +- v [ loop ]
          |      +- (cyclic)ABC
          |<-- -<<
          |v (TAIL-<<) [ ∅ ]
          |+- ABC
          |   :- v (HEAD) [ ∅ ]
          |   +- v [ loop ]
          |      +- (cyclic)ABC
        """.stripMargin

      linked.visualise().showStr(asciiArt = true) shouldBe
        """
          | ┌───────────────┐ ┌───────────────┐
          | │(TAIL>>-) [ ∅ ]│ │(TAIL-<<) [ ∅ ]│
          | └──────────────┬┘ └───────┬───────┘
          |                │          │
          |                │  ┌───────┘
          |                │  │
          |                v  v
          |             ┌───────┐
          |             │  ABC  │
          |             └─┬─┬───┘
          |               │ │ ^
          |               │ │ └──────┐
          |               │ └─────┐  │
          |               │       │  │
          |               v       v  │
          |   ┌────────────┐   ┌─────┴──┐
          |   │(HEAD) [ ∅ ]│   │[ loop ]│
          |   └────────────┘   └────────┘
        """.stripMargin
    }

    it("edge-node >>> itself") {

      val edge_node: Op = Edge(Some("loop")) :>> Node(Some("ABC"))

      val linked = edge_node :>> edge_node
      linked.visualise().showStr(asciiArt = true) shouldBe
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
          |               └─┬─┬───┘
          |                 │ │ ^
          |           ┌─────┘ │ └──────┐
          |           │       └─────┐  │
          |           │             │  │
          |           v             v  │
          |    ┌────────────┐    ┌─────┴──┐
          |    │(HEAD) [ ∅ ]│    │[ loop ]│
          |    └────────────┘    └────────┘
        """.stripMargin
    }

    it("(2 edges >- node ) >>> itself") {

      val edges_node = (Edge(Some("loop1")) U Edge(Some("loop2"))) :>> Node(Some("ABC"))

      val linked = edges_node :>> edges_node
      linked.visualise().showStr(asciiArt = true) shouldBe
        """
          | ┌───────────────────┐ ┌───────────────────┐ ┌───────────────┐
          | │(TAIL>>-) [ loop1 ]│ │(TAIL>>-) [ loop2 ]│ │(TAIL-<<) [ ∅ ]│
          | └─────────┬─────────┘ └──────┬────────────┘ └───────┬───────┘
          |           │                  │                      │
          |           └───────────────┐  │   ┌──────────────────┘
          |                           │  │   │
          |                           v  v   v
          |                        ┌───────────┐
          |                        │    ABC    │
          |                        └─┬────┬┬───┘
          |                          │    ││^^
          |             ┌────────────┘    │││└───────────────┐
          |             │                 │└┼─────────────┐  │
          |             │                 │ │             │  │
          |             v                 v │             v  │
          |      ┌────────────┐      ┌──────┴──┐      ┌──────┴──┐
          |      │(HEAD) [ ∅ ]│      │[ loop1 ]│      │[ loop2 ]│
          |      └────────────┘      └─────────┘      └─────────┘
        """.stripMargin
    }
  }

  describe("bidirectional") {

    it("node >>> node <<< node") {

      val n1: Op = Node(Some("A"))
      val n2: Op = Some("B")
      val n3: Op = Some("C")

      val rr = n1 :>> n2 <<: n3

      rr.visualise().showStr() shouldBe
        """
          |>>- -->
          |v (TAIL>>-) [ ∅ ]
          |+- A
          |   +- v [ ∅ ]
          |      +- B
          |         +- v (HEAD) [ ∅ ]
          |<-- -<<
          |v (TAIL-<<) [ ∅ ]
          |+- C
          |   +- v [ ∅ ]
          |      +- B
          |         +- v (HEAD) [ ∅ ]
      """.stripMargin

      rr.visualise()
        .showStr(true)
        .shouldBe(
          """
            | ┌───────────────┐ ┌───────────────┐
            | │(TAIL>>-) [ ∅ ]│ │(TAIL-<<) [ ∅ ]│
            | └────────┬──────┘ └───┬───────────┘
            |          │            │            
            |          v            v            
            |        ┌───┐        ┌───┐          
            |        │ A │        │ C │          
            |        └─┬─┘        └──┬┘          
            |          │             │           
            |          v             v           
            |       ┌─────┐       ┌─────┐        
            |       │[ ∅ ]│       │[ ∅ ]│        
            |       └──┬──┘       └──┬──┘        
            |          │             │           
            |          └─────┐ ┌─────┘           
            |                │ │                 
            |                v v                 
            |              ┌─────┐               
            |              │  B  │               
            |              └───┬─┘               
            |                  │                 
            |                  v                 
            |           ┌────────────┐           
            |           │(HEAD) [ ∅ ]│           
            |           └────────────┘           
            |""".stripMargin
        )
    }
  }
}
