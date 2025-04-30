package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.util.Causes
import com.tribbloids.spookystuff.testutils.BaseSpec

class SpookyExceptionSuite extends BaseSpec {

  describe(classOf[DFSReadException].getSimpleName) {

    val ee = new DFSReadException(
      "ee",
      Causes.combine(
        Seq(
          new AssertionError("2"),
          new AssertionError("1")
        )
      )
    )

    val ee_multipleLines = new DFSReadException(
      "ee1\n" + "ee2",
      Causes.combine(
        Seq(
          new AssertionError("2"),
          new AssertionError("1")
        )
      )
    )

    it("getMessage") {

      ee.getMessage.shouldBe(
        """
          |+ ee
          |!-- java.lang.AssertionError: 1
          |!-- java.lang.AssertionError: 2
        """.trim.stripMargin
      )
    }

    it("toString") {

      ee.toString.shouldBe(
        """
          |com.tribbloids.spookystuff.DFSReadException:
          |+ ee
          |!-- java.lang.AssertionError: 1
          |!-- java.lang.AssertionError: 2
        """.trim.stripMargin
      )
    }

    it("... multiple lines") {

      ee_multipleLines.toString.shouldBe(
        """
          |com.tribbloids.spookystuff.DFSReadException:
          |+ ee1
          |: ee2
          |!-- java.lang.AssertionError: 1
          |!-- java.lang.AssertionError: 2
        """.trim.stripMargin
      )
    }
  }
}
