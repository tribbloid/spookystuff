package com.tribbloids.spookystuff.uav.spatial

// to be replaced by actions.Rewriter[Trace]
object Anchors {

  // only used by LLA as AMSL anchor
  case object Geodetic extends Anchor

  // case object PlaceHolder extends Anchor

  // cast to UAVConf.HomeLocation
  case object Home extends Anchor

  // cast to UAV's projection to mean sea level (MSL)
  case object MSLProjection extends Anchor

  // cast to UAV's projection to home level
  case object HomeLevelProjection extends Anchor

  // cast to UAV's projection to ground terrain
  case object GroundProjection extends Anchor
}
