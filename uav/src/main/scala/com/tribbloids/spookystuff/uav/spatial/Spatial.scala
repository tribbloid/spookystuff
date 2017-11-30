package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.DenseVector
import com.tribbloids.spookystuff.uav.spatial.point.{CoordinateSystem, PointViewBase}
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory

trait Spatial[+T <: TrellisGeom] extends Serializable {

  def system: CoordinateSystem

  def geom: T
  def jtsGeom: JTSGeom = geom.jtsGeom
  def vector: DenseVector[Double] = DenseVector(jtsGeom.getCoordinates.flatMap(v => Seq(v.x, v.y, v.z)))

  var searchHistory: SearchHistory = _
}
//
object Spatial {

  implicit class PointView[C <: Coordinate](override val self: C) extends PointViewBase(self)
}
