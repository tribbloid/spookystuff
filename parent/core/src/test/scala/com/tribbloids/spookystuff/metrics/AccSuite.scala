package com.tribbloids.spookystuff.metrics

import com.tribbloids.spookystuff.testutils.FunSpecx
import org.apache.spark.util.LongAccumulator

class AccSuite extends FunSpecx {

  // TODO: it is not working
//  it("FromType") {
//
//    val acc = Acc.FromType[EventTimeStatsAccum]()
//
//    assert(acc.value == EventTimeStats.zero)
//  }

  it("Simple") {
    Acc.Simple(new LongAccumulator)
  }

  it("FromV0") {

    val acc = Acc.FromV0(0L)

    assert(acc.value == 0L)
  }

}
