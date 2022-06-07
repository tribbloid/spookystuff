package com.tribbloids.spookystuff.uav.spatial.point

import breeze.linalg.{Vector => Vec}
import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.Anchors
import com.tribbloids.spookystuff.utils.serialization.AssertSerializable

/**
  * Created by peng on 14/02/17.
  */
class LocationSuite extends SpookyEnvFixture {

  it("is serializable") {

    val location: Location = LLA.fromVec(Vec(-79.262262, 43.694195, 136)) -> Anchors.Geodetic
    AssertSerializable(location)
  }

  it("LLA + NED => LLA") {

    val p1: Location = LLA.fromVec(Vec(-79.262262, 43.694195, 136)) -> Anchors.Geodetic
    val p2: Location = NED.fromVec(Vec(1000, 2000, 30)) -> p1

    {
      val c2 = p2.getCoordinate(LLA, Anchors.Geodetic)
      c2.get.toStr_withSearchHistory.shouldBe(
        "LLA:POINT (-79.249854 43.712195 166) hops=1 recursions=2"
      )
    }

    {
      val c2 = p2.getCoordinate(LLA, p1)
      c2.get.toStr_withSearchHistory.shouldBe(
        "LLA:POINT (-79.249854 43.712195 30) hops=2 recursions=4"
      )
    }
  }

  it("LLA - LLA => NED") {
    val p1: Location = LLA.fromVec(Vec(-79.262262, 43.694195, 136)) -> Anchors.Geodetic
    val p2: Location = LLA.fromVec(Vec(-79.386132, 43.647023, 100)) -> Anchors.Geodetic

    val c2 = p2.getCoordinate(NED, p1)
    c2.get.toStr_withSearchHistory.shouldBe(
      "NED:POINT (-9993.849545 -5233.622679 -36) hops=1 recursions=3"
    )
  }

  it("NED - NED => NED") {
    {
      val base: Location = LLA(0, 0, 0) -> Anchors.Geodetic
      val p1: Location = NED(300, 200, 10) -> base
      val p2: Location = NED(100, 200, 30) -> base

      val c2 = p2.getCoordinate(NED, p1)
      c2.get.toStr_withSearchHistory.shouldBe(
        "NED:POINT (-0 -200 -20) hops=3 recursions=7"
      )
    }

    {
      val base: Location = UAVConf.DEFAULT_HOME_LOCATION
      val p1: Location = NED(300, 200, 10) -> base
      val p2: Location = NED(100, 200, 30) -> base

      val c2 = p2.getCoordinate(NED, p1)
      c2.get.toStr_withSearchHistory.shouldBe(
        "NED:POINT (-0.005983 -200 -20) hops=3 recursions=7" // TODO: should be 0
      )
    }
  }

  // TODO: enable this after reverse operator
  ignore("NED - NED => NED (Defined to Undetermined Anchor)") {
    val base = Anchors.Geodetic
    val p1: Location = NED(300, 200, 10) -> base
    val p2: Location = NED(100, 200, 30) -> base

    val c2 = p2.getCoordinate(NED, p1)
    c2.get.toStr_withSearchHistory.shouldBe(
      "NED:POINT (-0 -200 -20) hops=3 recursions=7"
    )
  }

  it("can handle cyclic referential NED") {
    val p1 = Location(Nil)

    val p2: Location = NED.fromVec(Vec(1000, 2000, 30)) -> p1

    p1.cache(NED.fromVec(Vec(-1000, -2000, -30)) -> p2)

    val c2 = p2.getCoordinate(LLA, Anchors.Geodetic)
    assert(c2.isEmpty)
  }

  it("can handle cyclic referential LLA") {
    val p1 = Location(Nil)

    val p2: Location = LLA.fromVec(Vec(-79.262262, 43.694195, 30)) -> p1

    p1.cache(LLA.fromVec(Vec(-79.262262, 43.694195, -30)) -> p2)

    val c2_Geo = p2.getCoordinate(LLA, Anchors.Geodetic)
    // TODO: should I use NaN and yield the known part?
    assert(c2_Geo.isEmpty)
  }

  it("can infer self referential NED from NED") {
    val p1 = Location(Nil)

    val p2: Location = NED.fromVec(Vec(1000, 2000, 30)) -> p1

    {
      val c2 = p1.getCoordinate(NED, p1)
      c2.get.toStr_withSearchHistory.shouldBe(
        "NED:POINT (0 0 -0) hops=0 recursions=0"
      )
    }

    {
      val c2 = p2.getCoordinate(NED, p2)
      c2.get.toStr_withSearchHistory.shouldBe(
        "NED:POINT (0 0 -0) hops=0 recursions=0"
      )
    }
  }

  it("can infer self referential NED from LLA") {
    val p1 = Location(Nil)

    val p2: Location = LLA.fromVec(Vec(-79.262262, 43.694195, 30)) -> p1

    {
      val c2 = p2.getCoordinate(LLA, p2)
      c2.get.toStr_withSearchHistory.shouldBe(
        "LLA:POINT (-79.262262 43.694195 0) hops=1 recursions=2"
      )
    }

    {
      val c2 = p2.getCoordinate(NED, p2)
      c2.get.toStr_withSearchHistory.shouldBe(
        "NED:POINT (0 0 -0) hops=0 recursions=0"
      )
    }
  }
}
