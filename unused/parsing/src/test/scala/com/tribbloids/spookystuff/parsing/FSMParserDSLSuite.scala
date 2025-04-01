package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.FSMParserDSL.*
import com.tribbloids.spookystuff.parsing.exception.ParsingError
import com.tribbloids.spookystuff.testutils.BaseSpec

class FSMParserDSLSuite extends BaseSpec {

  it("can form linear graph") {

    val p = P_*('$') :~> P('{').-- :~> P_*('}').!- :~> FINISH

    p.visualise()
      .ASCIIArt()
      .shouldBe(
        """
          | ╔═══════════════╗ ╔═══════════════╗
          | ║(TAIL>>-) [ ∅ ]║ ║(TAIL-<<) [ ∅ ]║
          | ╚══════════════╤╝ ╚═══════╤═══════╝
          |                │          │
          |                │ ┌────────┘
          |                │ │
          |                v v
          |              ╔═════╗
          |              ║ROOT ║
          |              ╚═══╤═╝
          |                  │
          |                  v
          |          ╔══════════════╗
          |          ║[ '$' [0...] ]║
          |          ╚══════╤═══════╝
          |                 │
          |                 v
          |               ╔═══╗
          |               ║---║
          |               ╚═╤═╝
          |                 │
          |                 v
          |           ╔═══════════╗
          |           ║[ '{' [0] ]║
          |           ╚═════╤═════╝
          |                 │
          |                 v
          |               ╔═══╗
          |               ║---║
          |               ╚══╤╝
          |                  │
          |                  v
          |          ╔══════════════╗
          |          ║[ '}' [0...] ]║
          |          ╚═══════╤══════╝
          |                  │
          |                  v
          |              ╔══════╗
          |              ║FINISH║
          |              ╚═══╤══╝
          |                  │
          |                  v
          |           ╔════════════╗
          |           ║(HEAD) [ ∅ ]║
          |           ╚════════════╝
        """.stripMargin
      )

    {
      p.parse("${abc}")
        .ioMapToString
        .shouldBe(
          """
          |$	-> $
          |{
          |abc}	-> abc
          |""".stripMargin
        )
    }

    {
      p.parse("abc${def}ghi")
        .ioMapToString
        .shouldBe(
          """
          |abc$	-> abc$
          |{
          |def}	-> def
          |""".stripMargin
        )
    }

    Seq(
      "ab",
      "$ab",
      "${ab"
    ).foreach { ii =>
      intercept[ParsingError] {
        println(p.parse(ii).ioMapToString)
      }
    }
  }

  describe("can form non-linear graph") {

    def validate(p: Operand[FSMParserGraph.Layout.GG]): Unit = {
      p.visualise()
        .ASCIIArt()
        .shouldBe(
          """
            | ╔═══════════════╗ ╔═══════════════╗
            | ║(TAIL>>-) [ ∅ ]║ ║(TAIL-<<) [ ∅ ]║
            | ╚══════════════╤╝ ╚═══════╤═══════╝
            |                │          │
            |                │ ┌────────┘
            |                │ │
            |                v v
            |              ╔═════╗
            |              ║ROOT ║
            |              ╚═══╤═╝
            |                  │
            |                  v
            |          ╔══════════════╗
            |          ║[ 'P' [0...] ]║
            |          ╚══════╤═══════╝
            |                 │
            |                 v
            |              ╔═════╗
            |              ║ --- ║
            |              ╚═╤═╤═╝
            |                │ │
            |         ┌──────┘ └──────┐
            |         │               │
            |         v               v
            |   ╔═══════════╗   ╔═══════════╗
            |   ║[ '1' [0] ]║   ║[ '2' [0] ]║
            |   ╚═════╤═════╝   ╚═════╤═════╝
            |         │               │
            |         └───────┐ ┌─────┘
            |                 │ │
            |                 v v
            |              ╔══════╗
            |              ║FINISH║
            |              ╚═══╤══╝
            |                  │
            |                  v
            |           ╔════════════╗
            |           ║(HEAD) [ ∅ ]║
            |           ╚════════════╝
        """.stripMargin
        )

      {
        p.parse("P123")
          .ioMapToString
          .shouldBe(
            """
              |P	-> P
              |1	-> 1
              |""".stripMargin
          )
      }

      {
        p.parse("P213")
          .ioMapToString
          .shouldBe(
            """
              |P	-> P
              |2	-> 2
              |""".stripMargin
          )
      }

      Seq(
        "Q1",
        "P3"
      ).foreach { ii =>
        intercept[ParsingError] {
          println(p.parse(ii).ioMapToString)
        }
      }

    }

    it(":~>") {

      val p = P_*('P') :~> (P('1') U P('2')) :~> FINISH
      validate(p)
    }

    it("<~:") {

      val p = FINISH <~: (P('1') U P('2')) <~: P_*('P')
      validate(p)
    }
  }

  it("can form loop") {

    val start = P_*('{').!-
    val p = start :~> P_*('}').!- :& start :~> EOS_* :~> FINISH
    p.visualise()
      .ASCIIArt()
      .shouldBe(
        """
          | ╔═══════════════╗ ╔═══════════════╗
          | ║(TAIL>>-) [ ∅ ]║ ║(TAIL-<<) [ ∅ ]║
          | ╚═══════╤═══════╝ ╚═╤═════════════╝
          |         │           │
          |         └────────┐  │
          |                  │  │
          |                  v  v
          |               ╔═══════╗
          |               ║ ROOT  ║
          |               ╚═╤═╤═══╝
          |                 │ │ ^
          |                 │ └─┼────────┐
          |                 │   │        │
          |                 v   │        v
          | ╔══════════════════╗│╔══════════════╗
          | ║[ '[EOS]' [0...] ]║│║[ '{' [0...] ]║
          | ╚════════╤═════════╝│╚═══════╤══════╝
          |          │          │        │
          |          │          │┌───────┘
          |          │          └┼───────┐
          |          v           v       │
          |      ╔══════╗      ╔═══╗     │
          |      ║FINISH║      ║---║     │
          |      ╚═══╤══╝      ╚═╤═╝     │
          |          │           │       │
          |          v           v       │
          |   ╔════════════╗   ╔═════════╧════╗
          |   ║(HEAD) [ ∅ ]║   ║[ '}' [0...] ]║
          |   ╚════════════╝   ╚══════════════╝
        """.stripMargin
      )

    {
      p.parse(" {abc}def")
        .ioMapToString
        .shouldBe(
          """
            | {	->
            |abc}	-> abc
            |def	-> def
            |""".stripMargin
        )
    }

    {
      p.parse(" {abc} {123} ")
        .ioMapToString
        .shouldBe(
          """
            | {	->
            |abc}	-> abc
            | {	->
            |123}	-> 123
            | 	->
            |""".stripMargin
        )
    }

    {
      p.parse(" {abc ")
        .ioMapToString
        .shouldBe(
          """
            | {abc 	->  {abc
            |""".stripMargin
        )
    }

    {
      p.parse(" {abc} {def")
        .ioMapToString
        .shouldBe(
          """
            | {	->
            |abc}	-> abc
            | {def	->  {def
            |""".stripMargin
        )
    }
  }

  it("can form self-loop") {

    val escape = P_*('\\')
    val p = escape :& escape :~> EOS_* :~> FINISH
    p.visualise()
      .ASCIIArt()
      .shouldBe(
        """
          | ╔═══════════════╗ ╔═══════════════╗
          | ║(TAIL>>-) [ ∅ ]║ ║(TAIL-<<) [ ∅ ]║
          | ╚══════════════╤╝ ╚═══════╤═══════╝
          |                │          │
          |                │  ┌───────┘
          |                │  │
          |                v  v
          |             ╔═══════╗
          |             ║ ROOT  ║
          |             ╚═╤═══╤═╝
          |               │ ^ │
          |               │ │ └───────┐
          |               │ └────────┐│
          |               v          ││
          |   ╔══════════════════╗   ││
          |   ║[ '[EOS]' [0...] ]║   ││
          |   ╚══════╤═══════════╝   ││
          |          │          ┌────┘│
          |          │          │    ┌┘
          |          v          │    │
          |      ╔══════╗       │    │
          |      ║FINISH║       │    │
          |      ╚═╤════╝       │    │
          |        │            │    │
          |        v            │    v
          | ╔════════════╗ ╔════╧═════════╗
          | ║(HEAD) [ ∅ ]║ ║[ '\' [0...] ]║
          | ╚════════════╝ ╚══════════════╝
        """.stripMargin
      )

    {
      p.parse("abc\\def\\\\ghi")
        .ioMapToString
        .shouldBe(
          """
            |abc\	-> abc\
            |def\	-> def\
            |\	-> \
            |ghi	-> ghi
            |""".stripMargin
        )
    }
  }

  it("self-loop can union with others") {
    val escape = P_*('\\')

    val _p = escape :& escape :~> P_*('$') :~> EOS_* :~> FINISH
    val p = _p U (EOS_* :~> FINISH)

    p.visualise()
      .ASCIIArt()
      .shouldBe(
        """
          |   ╔═══════════════╗   ╔═══════════════╗
          |   ║(TAIL>>-) [ ∅ ]║   ║(TAIL-<<) [ ∅ ]║
          |   ╚═══════╤═══════╝   ╚╤══════════════╝
          |           │            │
          |           └─────────┐  │
          |                     │  │
          |                     v  v
          |                 ╔═════════╗
          |                 ║  ROOT   ║
          |                 ╚═╤════╤╤═╝
          |                   │   ^││
          |            ┌──────┘   ││└─────────┐
          |            │          │└───────┐  │
          |            │          └───┐    │  │
          |            v              │    │  │
          |    ╔══════════════╗       │    │  │
          |    ║[ '$' [0...] ]║       │    │  │
          |    ╚════╤═════════╝       │    │  │
          |         │                 │    │  │
          |         v                 │    │  │
          |       ╔═══╗               │    │  │
          |       ║---║               │    │  │
          |       ╚═╤═╝               │    │  │
          |         │                 │    │  │
          |         │                 │    │  └─────────┐
          |         │                 └────┼──────────┐ │
          |         v                      v          │ │
          | ╔══════════════════╗ ╔══════════════════╗ │ │
          | ║[ '[EOS]' [0...] ]║ ║[ '[EOS]' [0...] ]║ │ │
          | ╚═════════╤════════╝ ╚═════════╤════════╝ │ │
          |           │                    │          │ │
          |           │ ┌──────────────────┘          │ │
          |           │ │               ┌─────────────┘ │
          |           v v               │               │
          |        ╔══════╗             │               │
          |        ║FINISH║             │               │
          |        ╚═══╤══╝             │               │
          |            │                │               │
          |            │                │    ┌──────────┘
          |            │                │    │
          |            v                │    v
          |     ╔════════════╗     ╔════╧═════════╗
          |     ║(HEAD) [ ∅ ]║     ║[ '\' [0...] ]║
          |     ╚════════════╝     ╚══════════════╝
        """.stripMargin
      )
  }
}
