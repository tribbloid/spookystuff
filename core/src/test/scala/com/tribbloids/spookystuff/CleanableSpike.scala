package com.tribbloids.spookystuff

import com.tribbloids.spookystuff.testutils.{FunSpecx, TestHelper}
import com.tribbloids.spookystuff.utils.WaitBeforeAppExit
import org.scalatest.Ignore

/**
  * Created by peng on 16/11/16.
  */
@Ignore
class CleanableSpike extends FunSpecx {

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