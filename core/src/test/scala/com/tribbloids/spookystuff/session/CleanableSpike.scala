package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.testutils.TestHelper
import com.tribbloids.spookystuff.utils.WaitBeforeAppExit
import org.scalatest.{FunSpec, Ignore}

/**
  * Created by peng on 16/11/16.
  */
@Ignore
class CleanableSpike extends FunSpec {

  it("wait for closing") {
    TestHelper.TestSC
      .parallelize(1 to 10)
      .map { v =>
        v * v
      }
      .collect()

    WaitBeforeAppExit.waitBeforeExit(20000000)
  }

}
