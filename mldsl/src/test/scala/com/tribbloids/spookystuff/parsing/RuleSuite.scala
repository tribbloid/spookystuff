package com.tribbloids.spookystuff.parsing

import org.scalatest.FunSpec

import FSMParserDSL._

class RuleSuite extends FunSpec {

  /**
    * TODO:
    * - test that entry node is automatically added to both tails if missing
    * - test that EOP node is automatically added to head if missing.
    * - use simple DSL to union >3 rules into 1 state
    * - test that SubRules cache is organised to include all cases
    */
  describe("add Start & End state automatically") {

    it("for 1 rule") {
      val r1 = P('$')

    }

    it("for 3 rules in 1 state") {}

    it("for 4 rules in a diamond pattern") {}
  }
}
