package com.tribbloids.spookystuff.parsing

import com.tribbloids.spookystuff.parsing.FSMParserDSL._
import com.tribbloids.spookystuff.testutils.FunSpecx
import com.tribbloids.spookystuff.utils.RangeArg

class ParsingRunSuite extends FunSpecx {

  /**
    * TODO:
    * - test that entry node is automatically added to both tails if missing
    * - test that EOP node is automatically added to head if missing.
    * - use simple DSL to union >3 rules into 1 state
    * - test that SubRules cache is organised to include all cases
    */
  describe("linear") {

    it("for 1 rule") {
      val p = P_*('$').!- :~> FINISH

      val cache = p.compiled.subRuleCache
      assert(cache.map(_._1) == Seq[RangeArg](0L to Long.MaxValue))

      p.parse("abcd$efg")
        .strRepr
        .shouldBe("abcd")
    }

    it("for 4 rules in 2 stages + EOS") {
      val a = P_*('<').!-
      val b1 = P('-').^^(v => Some("<-" -> v))
      val b2 = P_*('>').^^(v => Some("<.>" -> v))
      val b3 = P_*('|').^^(v => Some("<.|" -> v))

      val bs = (b1 U b2 U b3) :~> FINISH
      val p = (a :~> bs) U (EOS_* :~> FINISH)

      val cache = bs.compiled.subRuleCache
      assert(cache.map(_._1) == Seq[RangeArg](0L to 0L, 1L to Long.MaxValue))

      p.parse("xyz<-")
        .strRepr
        .shouldBe("""
            |xyz
            |(<-,-)
          """.stripMargin)

      p.parse("xyz<abc>|")
        .strRepr
        .shouldBe(
          """
            |xyz
            |(<.>,abc>)
          """.stripMargin
        )

      p.parse("xyz<12|34>")
        .strRepr
        .shouldBe("""
            |xyz
            |(<.|,12|)
          """.stripMargin)

      p.parse("abcd$efg")
        .strRepr
        .shouldBe("abcd$efg")
    }

    it("for 4 rules with diamond path") {

      val a1 = P_*('{').!- :~> P('{').--
      val a2 = P_*('(').!- :~> P('(').--

      val p = (a1 U a2) :~> EOS_* :~> FINISH

      p.parse("xyz{{12((34")
        .strRepr
        .shouldBe("""
                    |xyz
                    |12((34
                  """.stripMargin)

      p.parse("xyz((12{{34")
        .strRepr
        .shouldBe("""
                    |xyz
                    |12{{34
                  """.stripMargin)
    }
  }

  describe("loop") {

    it("escape by \\") {

      val escape = P_*('\\').--.% { _ =>
        PhaseVec.NoOp(1)
      }

      val _p = escape :& escape :~> P_*('$').!- :~> EOS_* :~> FINISH
      val p = _p U (EOS_* :~> FINISH)

      p.parse("abc$xyz")
        .strRepr
        .shouldBe("""
                    |abc
                    |xyz
                  """.stripMargin)

      p.parse("abc\\$xyz")
        .strRepr
        .shouldBe("""
                    |abc\$xyz
                  """.stripMargin)
    }

    it("multiple pair brackets") {

      val `{` = P_*('{').!-
      val p = `{` :~> P_*('}').!- :& `{` :~> EOS_* :~> FINISH

      p.parse("abc{def}ghi{jkl}mno{pqr}st")
        .strRepr
        .shouldBe(
          """
            |abc
            |def
            |ghi
            |jkl
            |mno
            |pqr
            |st
          """.stripMargin
        )
    }
  }

  describe("backtracking") {}

}
