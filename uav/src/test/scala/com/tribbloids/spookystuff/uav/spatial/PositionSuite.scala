package com.tribbloids.spookystuff.uav.spatial

import org.scalatest.FunSuite
import breeze.linalg.{Vector => Vec}
import com.tribbloids.spookystuff.testutils.{TestHelper, TestMixin}

/**
  * Created by peng on 14/02/17.
  */
class PositionSuite extends FunSuite with TestMixin {

  test("Position can infer LLA from NED") {

    val p1 = Position(
      Seq(
        LLA.V(Vec(-79.262262, 43.694195, 136)) -> GeodeticRef
      )
    )

    val p2 = Position(
      Seq(
        NED.V(Vec(1000,2000,30)) -> p1
      )
    )

    val c2_Geo = p2.getCoordinate(LLA, GeodeticRef)
    c2_Geo.get.toString.shouldBe(
      "LLA lat=43.71219508206757 lon=-79.24985395692833 alt=166.0"
    )
  }

  test("Position can infer LLA from LLA") {

    val p1 = Position(
      Seq(
        LLA.V(Vec(-79.262262, 43.694195, 136)) -> GeodeticRef
      )
    )

    val p2 = Position(
      Seq(
        NED.V(Vec(1000,2000,30)) -> p1
      )
    )

    val c2_Geo = p2.getCoordinate(LLA, p1)
    c2_Geo.get.toString.shouldBe(
      "LLA lat=43.71219508206757 lon=-79.24985395692833 alt=30.0"
    )
  }

  test("Position can infer NED from LLA") {
    val p1 = Position(
      Seq(
        LLA.V(Vec(-79.262262, 43.694195, 136)) -> GeodeticRef
      )
    )

    val p2 = Position(
      Seq(
        LLA.V(Vec(-79.386132, 43.647023, 100)) -> GeodeticRef
      )
    )

    val c2_Local = p2.getCoordinate(NED, p1)
    c2_Local.get.toString.shouldBe(
      "NED north=-5233.62267942232 east=-9993.849544672757 down=36.0"
    )
  }
}
