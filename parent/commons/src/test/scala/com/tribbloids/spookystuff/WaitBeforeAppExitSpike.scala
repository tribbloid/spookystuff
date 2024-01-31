package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.testutils.{BaseSpec, TestHelper}
import com.tribbloids.spookystuff.utils.WaitBeforeAppExit
import org.scalatest.Ignore

/**
  * Created by peng on 16/11/16.
  */
@Ignore
class WaitBeforeAppExitSpike extends BaseSpec {

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
