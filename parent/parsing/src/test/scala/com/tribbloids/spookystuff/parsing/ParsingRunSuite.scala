package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.FSMParserDSL._
import com.tribbloids.spookystuff.parsing.PhaseVec.{Depth, Eye, NoOp}
import com.tribbloids.spookystuff.testutils.BaseSpec
import com.tribbloids.spookystuff.commons.RangeMagnet

class ParsingRunSuite extends BaseSpec {

  /**
    * TODO:
    *   - test that entry node is automatically added to both tails if missing
    *   - test that EOP node is automatically added to head if missing.
    *   - use simple DSL to union >3 rules into 1 state
    *   - test that SubRules cache is organised to include all cases
    */
  describe("linear") {

    // TODO: should use ioMapToString in all assertions
    it("for 1 rule") {
      val p = P_*('$').!- :~> FINISH

      val cache = p.initialFState.subRuleCache
      assert(cache.map(_._1) == Seq[RangeMagnet](0L to Long.MaxValue))

      p.parse("abcd$efg")
        .ioMapToString
        .shouldBe("abcd$\t-> abcd")
    }

    it("for 4 rules in 2 stages + EOS") {
      val a = P_*('<').!-
      val b1 = P('-').^^(v => Some("<-" -> v.`export`.get))
      val b2 = P_*('>').^^(v => Some("<.>" -> v.`export`.get))
      val b3 = P_*('|').^^(v => Some("<.|" -> v.`export`.get))

      val bs = (b1 U b2 U b3) :~> FINISH
      val p = (a :~> bs) U (EOS_* :~> FINISH)

      val cache = bs.initialFState.subRuleCache
      assert(cache.map(_._1) == Seq[RangeMagnet](0L to 0L, 1L to Long.MaxValue))

      p.parse("xyz<-")
        .ioMapToString
        .shouldBe(
          """
            |xyz<	-> xyz
            |-	-> (<-,-)
          """.stripMargin
        )

      p.parse("xyz<abc>|")
        .ioMapToString
        .shouldBe(
          """
            |xyz<	-> xyz
            |abc>	-> (<.>,abc>)
          """.stripMargin
        )

      p.parse("xyz<12|34>")
        .ioMapToString
        .shouldBe("""
            |xyz<	-> xyz
            |12|	-> (<.|,12|)
          """.stripMargin)

      p.parse("abcd$efg")
        .ioMapToString
        .shouldBe("abcd$efg\t-> abcd$efg")
    }

    it("for 4 rules with diamond path") {

      val a1 = P_*('{').!- :~> P('{').--
      val a2 = P_*('(').!- :~> P('(').--

      val p = (a1 U a2) :~> EOS_* :~> FINISH

      p.parse("xyz{{12((34")
        .ioMapToString
        .shouldBe("""
                    |xyz{	-> xyz
                    |{
                    |12((34	-> 12((34
                  """.stripMargin)

      p.parse("xyz((12{{34")
        .ioMapToString
        .shouldBe("""
                    |xyz(	-> xyz
                    |(
                    |12{{34	-> 12{{34
                  """.stripMargin)
    }
  }

  describe("loop") {

    it("escape by \\") {

      val escape = ESC('\\')

      val _p = escape :~> P_*('$').!- :~> EOS_* :~> FINISH
      val p = _p U (EOS_* :~> FINISH)

//      val str = p.visualise().ASCIIArt.showStr

      p.parse("abc$xyz")
        .ioMapToString
        .shouldBe("""
                    |abc$	-> abc
                    |xyz	-> xyz
                  """.stripMargin)

      p.parse("abc\\$xyz")
        .ioMapToString
        .shouldBe("""
                    |abc\$xyz	-> abc\$xyz
                  """.stripMargin)

      p.parse("abc\\q$xyz")
        .ioMapToString
        .shouldBe("""
                    |abc\q$	-> abc\q
                    |xyz	-> xyz
                  """.stripMargin)
    }

    it("multiple pair brackets") {

      val `{` = P_*('{').!-
      val p = `{` :~> P_*('}').!- :& `{` :~> EOS_* :~> FINISH

      p.parse("abc{def}ghi{jkl}mno{pqr}st")
        .ioMapToString
        .shouldBe(
          """
            |abc{	-> abc
            |def}	-> def
            |ghi{	-> ghi
            |jkl}	-> jkl
            |mno{	-> mno
            |pqr}	-> pqr
            |st	-> st
          """.stripMargin
        )

      p.parse("abc{def}ghi{}jk")
        .ioMapToString
        .shouldBe(
          """
            |abc{	-> abc
            |def}	-> def
            |ghi{	-> ghi
            |}	->
            |jk	-> jk
          """.stripMargin
        )
    }
  }

  describe("backtracking") {

    it("unclosed bracket") {

      val `{` = P_*('{').!-
      val p = `{` :~> P_*('}').!- :& `{` :~> EOS_* :~> FINISH

      p.parse("abc{def}ghi{jk")
        .ioMapToString
        .shouldBe(
          """
            |abc{	-> abc
            |def}	-> def
            |ghi{jk	-> ghi{jk
          """.stripMargin
        )
    }
  }

  describe("conditional") {

    it("can parse paired brackets") {

      val entry = P_*('P').!- :~> P('{').--.% { _ =>
        Depth(0)
      }

      val more = P_*('{') % { io =>
        io.nextPhaseVec match {
          case Depth(i) => Depth(i + 1)
        }
      }

      val less = P_*('}') % { io =>
        io.nextPhaseVec match {
          case Depth(0) => NoOp
          case Depth(i) => Depth(i - 1)
        }
      }

      val moreOrLess = more U less

      val out = P_*('}').!- % { io =>
        io.nextPhaseVec match {
          case Depth(0) => Eye
          case Depth(_) => NoOp
        }
      }

      val p: Operand[FSMParserGraph.Layout.GG] = (entry :~> moreOrLess :& moreOrLess) :~>
        out :& entry :~> EOS_* :~> FINISH

      p.visualise()
        .ASCIIArt
        .showStr
        .shouldBe(
          """
          |          ╔═══════════════╗          ╔═══════════════╗            
          |          ║(TAIL>>-) [ ∅ ]║          ║(TAIL-<<) [ ∅ ]║            
          |          ╚═══════╤═══════╝          ╚═══════╤═══════╝            
          |                  │                          │                    
          |                  └────────────┐  ┌──────────┘                    
          |                               │  │                               
          |                               v  v                               
          |                            ╔═══════╗                             
          |                            ║ ROOT  ║                             
          |                            ╚═╤═══╤═╝                             
          |                              │  ^│                               
          |                   ┌──────────┘  │└──────────┐                    
          |                   │       ┌─────┘           │                    
          |                   v       │                 │                    
          |           ╔══════════════╗│                 │                    
          |           ║[ 'P' [0...] ]║│                 │                    
          |           ╚════╤═════════╝│                 │                    
          |                │          │                 │                    
          |                v          │                 │                    
          |              ╔═══╗        │                 │                    
          |              ║---║        │                 │                    
          |              ╚═╤═╝        │                 │                    
          |                │          │                 │                    
          |                v          │                 v                    
          |       ╔═══════════╗       │       ╔══════════════════╗           
          |       ║[ '{' [0] ]║       │       ║[ '[EOS]' [0...] ]║           
          |       ╚════════╤══╝       │       ╚════════════╤═════╝           
          |                │          │                    │                 
          |                v          │                    v                 
          |          ╔═══════════╗    │                ╔══════╗              
          |          ║    ---    ║    │                ║FINISH║              
          |          ╚═╤═══╤═══╤═╝    │                ╚═══╤══╝              
          |           ^│   │ ^ │      │                    │                 
          |           ││   │ │ │      │                    └──────────┐      
          |           ││   │ │ │      └─────────────────┐             │      
          |           ││   │ │ └───────────────────┐    │             │      
          |           ││   │ └─────────┐           │    │             │      
          |           ││   └──────┐    │           │    │             │      
          |           ││          │    │           │    │             │      
          |           │v          v    │           v    │             v      
          | ╔═════════╧════╗ ╔═════════╧════╗ ╔═════════╧════╗ ╔════════════╗
          | ║[ '{' [0...] ]║ ║[ '}' [0...] ]║ ║[ '}' [0...] ]║ ║(HEAD) [ ∅ ]║
          | ╚══════════════╝ ╚══════════════╝ ╚══════════════╝ ╚════════════╝
          |""".stripMargin
        )

      p.parse("aBCDP{12} b")
        .ioMapToString
        .shouldBe(
          """
            |aBCDP	-> aBCD
            |{
            |12}	-> 12
            | b	->  b
          """.stripMargin
        )

      p.parse("aBCDP{12{34}56}EFG")
        .ioMapToString
        .shouldBe(
          """
            |aBCDP	-> aBCD
            |{
            |12{	-> 12{
            |34}	-> 34}
            |56}	-> 56
            |EFG	-> EFG
          """.stripMargin
        )

      p.parse("aP{12{34}56}BCP{78}D")
        .ioMapToString
        .shouldBe(
          """
            |aP	-> a
            |{
            |12{	-> 12{
            |34}	-> 34}
            |56}	-> 56
            |BCP	-> BC
            |{
            |78}	-> 78
            |D	-> D
          """.stripMargin
        )
    }

  }
}

object ParsingRunSuite {}
