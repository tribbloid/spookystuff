package com.tribbloids.spookystuff.uav.spatial.point

import com.tribbloids.spookystuff.uav.spatial.{Anchor, JTSPoint, Spatial, TrellisPoint}
import com.tribbloids.spookystuff.uav.spatial.util.{SearchAttempt, SearchHistory}
import org.apache.spark.mllib.uav.Vec
import org.osgeo.proj4j.ProjCoordinate

import scala.language.implicitConversions

class PointViewBase(val self: Spatial[TrellisPoint]) {

  def system: CoordinateSystem = self.system
  def point: JTSPoint = self.geom.jtsGeom

  def x: Double = point.getX
  def y: Double = point.getY
  def z: Double = point.getCoordinate.z

  /**
    * implement this to bypass proj4
    */
  def fastProject(
                   ref1: Anchor,
                   ref2: Anchor,
                   system2: CoordinateSystem,
                   ic: SearchHistory
                 ): Option[system2.Coordinate] = {
    system2 match {
      case NED if ref1 == ref2 => Some(NED(0,0,0).asInstanceOf[system2.Coordinate])
      case _ => None
    }
  }

  def projectZ(ref1: Anchor, ref2: Anchor, system2: CoordinateSystem, ic: SearchHistory): Option[Double] = {

    val delta2_1Opt = {
      ic.getCoordinate(SearchAttempt(ref2, LLA, ref1))
        .map(_.alt)
        .orElse {
          ic.getCoordinate(SearchAttempt(ref1, LLA, ref2))
            .map(v => - v.alt)
        }
    }

    delta2_1Opt.map {
      delta =>
        delta + z
    }
  }

  /**
    * @param ref2 origin of the new coordinate system
    * @param system2 type of the new coordinate system
    * @return new coordinate
    */
  def project(
               ref1: Anchor,
               ref2: Anchor,
               system2: CoordinateSystem,
               ic: SearchHistory
             ): Option[system2.Coordinate] = {

    val customResult: Option[system2.Coordinate] = fastProject(ref1, ref2, system2, ic)
      .map(_.asInstanceOf[system2.Coordinate]) // redundant! IDE error?
    customResult.orElse {
      val dstZOpt = projectZ(ref1, ref2, system2, ic)
      for (
        proj1 <- system.get2DProj(ref1, ic);
        proj2 <- system2.get2DProj(ref2, ic);
        dstZ <- dstZOpt
      ) yield {
        val src = new ProjCoordinate(x, y)
        val wgs84 = new ProjCoordinate
        val dst = new ProjCoordinate
        proj1.inverseProject(src, wgs84)
        proj2.project(wgs84, dst)
        val vec: Vec = Vec(dst.x, dst.y, dstZ)
        system2.fromVec(vec, ic)
      }
    }
  }
}
