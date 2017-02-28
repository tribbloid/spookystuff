package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.{Vector => Vec}
import com.tribbloids.spookystuff.testutils.TestMixin
import com.tribbloids.spookystuff.uav.UAVConf
import org.scalatest.FunSuite

/**
  * Created by peng on 14/02/17.
  */
class LocationSuite extends FunSuite with TestMixin {

  test("Location can infer LLA from NED") {

    val p1 : Location = LLA.create(Vec(-79.262262, 43.694195, 136)) -> GeodeticAnchor

    val p2 : Location = NED.create(Vec(1000, 2000, 30)) -> p1

    val c2 = p2.getCoordinate(LLA, GeodeticAnchor)
    c2.get.withICString.shouldBe(
      "LLA lat=43.712195 lon=-79.249854 alt=166.000000 hops=1 recursions=2"
    )
  }

  test("Location can infer LLA from LLA") {

    val p1 : Location = LLA.create(Vec(-79.262262, 43.694195, 136)) -> GeodeticAnchor

    val p2 : Location = NED.create(Vec(1000, 2000, 30)) -> p1

    val c2 = p2.getCoordinate(LLA, p1)
    c2.get.withICString.shouldBe(
      "LLA lat=43.712195 lon=-79.249854 alt=30.000000 hops=2 recursions=4"
    )
  }

  test("Location can infer NED from LLA") {
    val p1 : Location = LLA.create(Vec(-79.262262, 43.694195, 136)) -> GeodeticAnchor

    val p2 : Location = LLA.create(Vec(-79.386132, 43.647023, 100)) -> GeodeticAnchor

    val c2 = p2.getCoordinate(NED, p1)
    c2.get.withICString.shouldBe(
      "NED north=-5233.622679 east=-9993.849545 down=36.000000 hops=1 recursions=3"
    )
  }

  test("Location can infer NED from NED") {
    {
      val base: Location = LLA(0, 0, 0) -> GeodeticAnchor
      val p1 : Location = NED(300, 200, 10) -> base
      val p2 : Location = NED(100, 200, 30) -> base

      val c2 = p2.getCoordinate(NED, p1)
      c2.get.withICString.shouldBe(
        "NED north=-200.000000 east=-0.000000 down=20.000000 hops=3 recursions=7"
      )
    }

    {
      val base: Location = UAVConf.HOME_LOCATION
      val p1 : Location = NED(300, 200, 10) -> base
      val p2 : Location = NED(100, 200, 30) -> base

      val c2 = p2.getCoordinate(NED, p1)
      c2.get.withICString.shouldBe(
        "NED north=-200.000000 east=-0.005983 down=20.000000 hops=3 recursions=7"
      )
    }
  }

  test("Location can handle cyclic referential NED") {
    val p1 = Location(Nil)

    val p2 : Location = NED.create(Vec(1000, 2000, 30)) -> p1

    p1.addCoordinate(Relation(NED.create(Vec(-1000,-2000,-30)), p2))

    val c2 = p2.getCoordinate(LLA, GeodeticAnchor)
    assert(c2.isEmpty)
  }

  test("Location can handle cyclic referential LLA") {
    val p1 = Location(Nil)

    val p2 : Location = LLA.create(Vec(-79.262262, 43.694195, 30)) -> p1

    p1.addCoordinate(Relation(LLA.create(Vec(-79.262262, 43.694195,-30)), p2))

    val c2_Geo = p2.getCoordinate(LLA, GeodeticAnchor)
    //TODO: should I use NaN and yield the known part?
    assert(c2_Geo.isEmpty)
  }

  test("Location can infer self referential NED from NED") {
    val p1 = Location(Nil)

    val p2 : Location = NED.create(Vec(1000, 2000, 30)) -> p1

    {
      val c2 = p1.getCoordinate(NED, p1)
      c2.get.withICString.shouldBe(
        "NED north=0.000000 east=0.000000 down=-0.000000 hops=0 recursions=0"
      )
    }

    {
      val c2 = p2.getCoordinate(NED, p2)
      c2.get.withICString.shouldBe(
        "NED north=0.000000 east=0.000000 down=-0.000000 hops=0 recursions=0"
      )
    }
  }

  test("Location can infer self referential NED from LLA") {
    val p1 = Location(Nil)

    val p2 : Location = LLA.create(Vec(-79.262262, 43.694195, 30)) -> p1

    {
      val c2 = p2.getCoordinate(LLA, p2)
      c2.get.withICString.shouldBe(
        "LLA lat=43.694195 lon=-79.262262 alt=0.000000 hops=1 recursions=2"
      )
    }

    {
      val c2 = p2.getCoordinate(NED, p2)
      c2.get.withICString.shouldBe(
        "NED north=0.000000 east=0.000000 down=-0.000000 hops=0 recursions=0"
      )
    }
  }
}