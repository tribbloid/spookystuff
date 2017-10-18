package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.spatial.point.NED
import org.apache.spark.mllib.uav.ClearanceGradient

class CollisionAvoidanceSuite extends SpookyEnvFixture {

  describe("t4MinimalDist can solve arg min t1 & t2") {

    it("example 1") {
      val A1 = NED.C(1,0,0)
      val B1 = NED.C(0,1,0)
      val A2 = NED.C(0,0,-1)
      val B2 = NED.C(1,1,0)

      val (t1, t2) = ClearanceGradient.t4MinimalDist(A1, B1, A2, B2)
      assert(t1 === 1.0/2)
      assert(t2 === 2.0/3)
    }

    it("example 2") {
      val A1 = NED.C(2,0,0)
      val B1 = NED.C(0,1,0)
      val A2 = NED.C(0,0,-2)
      val B2 = NED.C(1,1,0)

      val (t1, t2) = ClearanceGradient.t4MinimalDist(A1, B1, A2, B2)
      assert(t1 === 18.0/29)
      assert(t2 === 26.0/29)
    }
  }

}
