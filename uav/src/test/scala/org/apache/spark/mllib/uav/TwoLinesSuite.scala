package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.spatial.point.NED
import org.apache.spark.ml.uav.TwoLines

class TwoLinesSuite extends SpookyEnvFixture {

  describe("can solve arg min t1 & t2") {

    it("example 1") {
      val A1 = NED.apply(1,0,0)
      val B1 = NED.apply(0,1,0)
      val A2 = NED.apply(0,0,-1)
      val B2 = NED.apply(1,1,0)

      val v = TwoLines(A1, B1, A2, B2)
      import v._
      assert(t1 === 1.0/2)
      assert(t2 === 2.0/3)
    }

    it("example 2") {
      val A1 = NED.apply(2,0,0)
      val B1 = NED.apply(0,1,0)
      val A2 = NED.apply(0,0,-2)
      val B2 = NED.apply(1,1,0)

      val v = TwoLines(A1, B1, A2, B2)
      import v._
      assert(t1 === 18.0/29)
      assert(t2 === 26.0/29)
    }

    it("when A1 = B1") {
      val A1 = NED.apply(1,0,0)
      val B1 = NED.apply(1,0,0)
      val A2 = NED.apply(0,0,-1)
      val B2 = NED.apply(1,1,0)

      val v = TwoLines(A1, B1, A2, B2)
      import v._
      assert(t1 === 0.0)
      assert(t2 === 2.0/3)
    }
    it("when A2 = B2") {
      val A1 = NED.apply(1,0,0)
      val B1 = NED.apply(0,1,0)
      val A2 = NED.apply(1,1,0)
      val B2 = NED.apply(1,1,0)

      val v = TwoLines(A1, B1, A2, B2)
      import v._
      assert(t1 === 0.5)
      assert(t2 === 0.0)
    }

    it("when A1 = A2") {
      val A1 = NED.apply(1,0,0)
      val B1 = NED.apply(0,1,0)
      val A2 = NED.apply(1,0,0)
      val B2 = NED.apply(1,1,0)

      val v = TwoLines(A1, B1, A2, B2)
      import v._
      assert(t1 === 0.0)
      assert(t2 === 0.0)
    }

    it("when A1B1 // A2B2") {
      val A1 = NED.apply(1,0,0)
      val B1 = NED.apply(0,1,0)
      val A2 = NED.apply(2,0,0)
      val B2 = NED.apply(1,1,0)

      val v = TwoLines(A1, B1, A2, B2)
      import v._
      assert(t1 === 0.0)
      assert(t2 === 0.0)
    }

    it("when A1B1 == A2B2") {
      val A1 = NED.apply(1,0,0)
      val B1 = NED.apply(0,1,0)
      val A2 = NED.apply(1,0,0)
      val B2 = NED.apply(0,1,0)

      val v = TwoLines(A1, B1, A2, B2)
      import v._
      assert(t1 === 0.0)
      assert(t2 === 0.0)
    }
  }
}
