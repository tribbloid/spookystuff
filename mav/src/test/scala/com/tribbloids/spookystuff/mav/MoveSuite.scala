package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.mav.actions.GlobalLocation
import com.tribbloids.spookystuff.mav.actions.Move

/**
  * Created by peng on 26/08/16.
  */
class MoveSuite extends SpookyEnvFixture {

  test("Move should support toJson") {
    val wp1 = GlobalLocation(0,0,0)
    val wp2 = GlobalLocation(20, 30, 50)

    val move = Move(wp1, wp2)

    println(move.prettyJson)
  }
}
