package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.spark.TestHelper
import com.tribbloids.spookystuff.commons.WaitBeforeAppExit
import com.tribbloids.spookystuff.testutils.BaseSpec
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
