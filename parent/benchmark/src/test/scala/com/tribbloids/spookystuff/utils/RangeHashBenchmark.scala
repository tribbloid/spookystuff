package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.commons.lang.math.{IntRange, LongRange}

class RangeHashBenchmark extends FunSpecx {

  it("range hash should be fast") {

    Seq(
//      CommonUtils.timed((0 until Int.MaxValue).hashCode()), // this will take forever
      CommonUtils.timed((0, Int.MaxValue).hashCode()),
      CommonUtils.timed(new IntRange(0, Int.MaxValue).hashCode()),
      CommonUtils.timed(new LongRange(0, Int.MaxValue).hashCode())
    )
  }
}
