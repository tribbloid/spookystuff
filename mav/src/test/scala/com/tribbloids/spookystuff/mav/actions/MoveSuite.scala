package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.extractors.Literal
import com.tribbloids.spookystuff.mav.sim.APMSimFixture

/**
  * Created by peng on 26/08/16.
  */
class MoveSuite extends APMSimFixture {

  test("Move.toJson should work") {
    val wp1 = Global(0,0,0)
    val wp2 = Global(20, 30, 50)

    val move = Move(Literal(wp1), Literal(wp2))

    println(move.toMessage.prettyJSON())
  }

//  test("Move can ")
}
