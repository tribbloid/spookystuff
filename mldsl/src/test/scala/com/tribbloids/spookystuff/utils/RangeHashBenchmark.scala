package com.tribbloids.spookystuff.utils

import org.apache.commons.lang.math.{IntRange, LongRange}
import org.scalatest.FunSpec

class RangeHashBenchmark extends FunSpec {

  it("range hash should be fast") {

    val results = Seq(
      CommonUtils.timed((0 until Int.MaxValue).hashCode()),
      CommonUtils.timed((0, Int.MaxValue).hashCode()),
      CommonUtils.timed(new IntRange(0, Int.MaxValue).hashCode()),
      CommonUtils.timed(new LongRange(0, Int.MaxValue).hashCode())
    )

    print(results.mkString("\n"))
  }
}
