package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.uav.sim.APMSim
import org.osgeo.proj4j.datum.Ellipsoid
import org.osgeo.proj4j.proj.EquidistantAzimuthalProjection
import org.osgeo.proj4j.{CRSFactory, ProjCoordinate}
import org.scalatest.FunSuite

/**
  * Created by peng on 12/02/17.
  */
class TestProj4 extends FunSuite {

  protected def factory = new CRSFactory

  test("Local to Global by projection") {

    val ref1 = APMSim.HOME_LLA

    val projs = Seq(
      {
        val proj = new EquidistantAzimuthalProjection(Math.toRadians(ref1.lat), Math.toRadians(ref1.lon))
//        proj.setUnits(Units.METRES)
        proj.setEllipsoid(Ellipsoid.WGS84)
        proj.initialize()
        proj
      },
      {
        val crs = factory.createFromParameters("a", s"+proj=aeqd +lat_0=${ref1.lat} +lon_0=${ref1.lon}")
        crs.getProjection
      }
    )

    projs.foreach {
      proj =>
        println(proj)
        proj.setUnits(org.osgeo.proj4j.units.Units.METRES)
        val src = new ProjCoordinate(1000, 2000, 3000)
        val dst = new ProjCoordinate
        proj.inverseProject(src, dst) // NED -> WGS84
        val result = (dst.y, dst.x, dst.z)
        println(s"origin: ${ref1.lat -> ref1.lon}, actual: $result")
    }
  }
}
