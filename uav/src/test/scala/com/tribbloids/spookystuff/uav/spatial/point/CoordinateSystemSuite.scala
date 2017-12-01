package com.tribbloids.spookystuff.uav.spatial.point

import com.tribbloids.spookystuff.testutils.FunSpecx

abstract class CoordinateSystemSuite extends FunSpecx {

  val cs: CoordinateSystem

  it("toRepr() should be idempotent") {

    val c = cs.fromXYZ(20, 30, 40)
    val c1 = c: cs.Repr
    val c2 = cs.toRepr(c1)

    assert(c == c1)
    assert(c1 == c2)
  }
}

class NEDSuite extends CoordinateSystemSuite {
  override val cs: CoordinateSystem = NED
}

class LLASuite extends CoordinateSystemSuite {
  override val cs: CoordinateSystem = LLA
}