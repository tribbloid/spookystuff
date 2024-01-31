package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.testutils.BaseSpec

class RangeHashBenchmark extends BaseSpec {

  it("RangeArg hash should be fast") { // TODO: move to unit test

    Seq(
//      CommonUtils.timed((0 until Int.MaxValue).hashCode()), // this will take forever
      CommonUtils.timed((0, Int.MaxValue).hashCode()),
      CommonUtils.timed(RangeMagnet(0, Int.MaxValue).hashCode())
    )
  }
}
