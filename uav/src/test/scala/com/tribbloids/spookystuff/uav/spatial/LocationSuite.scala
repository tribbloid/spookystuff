package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.{Vector => Vec}
import com.tribbloids.spookystuff.testutils.TestMixin
import org.scalatest.FunSuite

/**
  * Created by peng on 14/02/17.
  */
class LocationSuite extends FunSuite with TestMixin {

  test("Location can infer LLA from NED") {

    val p1 : Location = LLA.create(Vec(-79.262262, 43.694195, 136)) -> GeodeticAnchor

    val p2 : Location = NED.create(Vec(1000, 2000, 30)) -> p1

    val c2 = p2.getCoordinate(LLA, GeodeticAnchor)
    c2.get.toString.shouldBe(
      "LLA lat=43.71219508206757 lon=-79.24985395692833 alt=166.0"
    )
  }

  test("Location can infer LLA from LLA") {

    val p1 : Location = LLA.create(Vec(-79.262262, 43.694195, 136)) -> GeodeticAnchor

    val p2 : Location = NED.create(Vec(1000, 2000, 30)) -> p1

    val c2 = p2.getCoordinate(LLA, p1)
    c2.get.toString.shouldBe(
      "LLA lat=43.71219508206757 lon=-79.24985395692833 alt=30.0"
    )
  }

  test("Location can infer NED from LLA") {
    val p1 : Location = LLA.create(Vec(-79.262262, 43.694195, 136)) -> GeodeticAnchor

    val p2 : Location = LLA.create(Vec(-79.386132, 43.647023, 100)) -> GeodeticAnchor

    val c2 = p2.getCoordinate(NED, p1)
    c2.get.toString.shouldBe(
      "NED north=-5233.62267942232 east=-9993.849544672757 down=36.0"
    )
  }

  test("Location can handle cyclic referential NED") {
    val p1 = Location(Nil)

    val p2 : Location = NED.create(Vec(1000, 2000, 30)) -> p1

    p1.addCoordinate(Denotation(NED.create(Vec(-1000,-2000,-30)), p2))

    val c2 = p2.getCoordinate(LLA, GeodeticAnchor)
    assert(c2.isEmpty)
  }

  test("Location can handle cyclic referential LLA") {
    val p1 = Location(Nil)

    val p2 : Location = LLA.create(Vec(-79.262262, 43.694195, 30)) -> p1

    p1.addCoordinate(Denotation(LLA.create(Vec(-79.262262, 43.694195,-30)), p2))

    val c2_Geo = p2.getCoordinate(LLA, GeodeticAnchor)
    //TODO: should I use NaN and yield the known part?
    assert(c2_Geo.isEmpty)
  }

  test("Location can handle self referential NED") {
    val p1 = Location(Nil)

    val p2 : Location = NED.create(Vec(1000, 2000, 30)) -> p1

    {
      val c2 = p1.getCoordinate(NED, p1)
      c2.get.toString.shouldBe(
        "NED north=0.0 east=0.0 down=0.0"
      )
    }

    {
      val c2 = p2.getCoordinate(NED, p2)
      c2.get.toString.shouldBe(
        "NED north=0.0 east=0.0 down=0.0"
      )
    }
  }

  test("Location can handle self referential LLA") {
    val p1 = Location(Nil)

    val p2 : Location = LLA.create(Vec(-79.262262, 43.694195, 30)) -> p1

    {
      val c2 = p2.getCoordinate(LLA, p2)
      c2.get.toString.shouldBe(
        "LLA lat=43.694195 lon=-79.262262 alt=0.0"
      )
    }

    {
      val c2 = p2.getCoordinate(NED, p2)
      c2.get.toString.shouldBe(
        "NED north=0.0 east=0.0 down=0.0"
      )
    }
  }
}