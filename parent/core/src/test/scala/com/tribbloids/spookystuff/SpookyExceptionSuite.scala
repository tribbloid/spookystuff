package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.testutils.BaseSpec
import com.tribbloids.spookystuff.utils.TreeThrowable

class SpookyExceptionSuite extends BaseSpec {

  describe("DFSReadException") {
    it(".getMessage contains causes") {

      val ee = new DFSReadException(
        "ee",
        TreeThrowable.combine(
          Seq(
            new AssertionError("2"),
            new AssertionError("1")
          )
        )
      )

      ee.getMessage.shouldBe(
        """
          |ee
          |:- java.lang.AssertionError: 1
          |+- java.lang.AssertionError: 2
          |
        """.trim.stripMargin
      )

      ee.toString.shouldBe(
        """
          |com.tribbloids.spookystuff.DFSReadException: ee
          |:- java.lang.AssertionError: 1
          |+- java.lang.AssertionError: 2
        """.trim.stripMargin
      )
    }
  }
}
