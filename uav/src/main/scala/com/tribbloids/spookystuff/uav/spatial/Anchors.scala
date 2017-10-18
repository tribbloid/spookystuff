package com.tribbloids.spookystuff.uav.spatial

// to be replaced by actions.Rewriter[Trace]
object Anchors {

  // only used by LLA as AMSL anchor
  case object Geodetic extends UndeterminedLike

  // cast to UAVConf.HomeLocation
  case object Home extends UndeterminedLike

  // cast to UAV's projection to mean sea level (MSL)
  case object MSLProjection extends UndeterminedLike

  // cast to UAV's projection to home level
  case object HomeLevelProjection extends UndeterminedLike

  // cast to UAV's projection to ground terrain
  case object GroundProjection extends UndeterminedLike

  case class Custom(name: String) extends UndeterminedLike
}
