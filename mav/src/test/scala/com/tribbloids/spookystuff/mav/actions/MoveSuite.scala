package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.extractors.Literal

/**
  * Created by peng on 26/08/16.
  */
class MoveSuite extends SpookyEnvFixture {

  test("Move should support toJson") {
    val wp1 = Global(0,0,0)
    val wp2 = Global(20, 30, 50)

    val move = Move(Literal(wp1), Literal(wp2))

    println(move.toMessage.prettyJSON())
  }
}
