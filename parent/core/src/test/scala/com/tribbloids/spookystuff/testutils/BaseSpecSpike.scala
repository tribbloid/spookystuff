package com.tribbloids.spookystuff.testutils

import ai.acyclic.prover.commons.diff.StringDiff

class BaseSpecSpike extends BaseSpec {

  ignore("diff in IDE") {

    val left =
      """
        |c1
        |  c2
        |    c3
        |  c4
        |""".stripMargin

    val right =
      """
        |c1
        |  d2
        |    c3
        |    c4
        |""".stripMargin

    it("1") {

      left shouldBe right
    }

    it("2") {

      val diff = StringDiff(Some(left), Some(right))

      diff.assert()
    }
  }

}
