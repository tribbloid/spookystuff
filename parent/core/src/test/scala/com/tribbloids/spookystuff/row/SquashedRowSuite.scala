package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.testutils.SpookyBaseSpec

/**
  * Created by peng on 05/04/16.
  */
class SquashedRowSuite extends SpookyBaseSpec {

//  it("execution yields at least 1 trajectory") {
//    val row = Row.blank.asSquashed
//    val grouped = row.batch.map(_.sourceScope)
//    assert(grouped == Seq(Seq()))
//  }

  describe("withCtx") {

    it("will not generate duplicated objects") {

      val row = BuildRow("abc").squashed

      val w1 = row.withCtx(spooky)
      val w2 = row.withCtx(spooky)

      assert(w1 eq w2)
    }
  }
}
