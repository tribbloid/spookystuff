package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.testutils.FunSpecx
import com.tribbloids.spookystuff.utils.TreeException

class SpookyExceptionSuite extends FunSpecx{

  describe("DFSReadException") {
    it(".getMessage contains causes") {

      val ee = new DFSReadException(
        "ee",
        TreeException.wrap(Seq(
          new AssertionError("2"),
          new AssertionError("1")
        ))
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
