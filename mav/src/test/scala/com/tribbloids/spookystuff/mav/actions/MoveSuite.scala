package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.mav.MavFixture

/**
  * Created by peng on 26/08/16.
  */
class MoveSuite extends MavFixture {

  test("Move should support toJson") {
    val wp1 = WayPoint(Some(GlobalLocation(0,0,0)))
    val wp2 = WayPoint(Some(GlobalLocation(20, 30, 50)))

    val move = Move(wp1, wp2)

    println(move.toMessage.prettyJSON())
  }
}
