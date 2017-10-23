package com.tribbloids.spookystuff.uav.spatial

// to be replaced by actions.Rewriter[Trace]
object Anchors {

  trait Tag extends UndeterminedLike {
    def name = this.getClass.getSimpleName.stripSuffix("$")
  }
  // only used by LLA as AMSL anchor
  case object Geodetic extends Tag

  // cast to UAVConf.HomeLocation
  case object Home extends Tag

  // cast to UAV's projection to mean sea level (MSL)
  case object MSLProjection extends Tag

  // cast to UAV's projection to home level
  case object HomeLevelProjection extends Tag

  // cast to UAV's projection to ground terrain
  case object GroundProjection extends Tag

  case class Named(override val name: String) extends Tag
}
