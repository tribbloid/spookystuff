package com.tribbloids.spookystuff.uav.spatial.point

import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory

import scala.language.implicitConversions

class PointViewBase(val self: GeomSpatial[TrellisPoint]) {

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


}
