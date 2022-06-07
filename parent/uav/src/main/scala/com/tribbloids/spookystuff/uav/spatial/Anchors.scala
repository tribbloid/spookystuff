package com.tribbloids.spookystuff.uav.spatial

// to be replaced by actions.Rewriter[Trace]
object Anchors {

  trait Alias extends UndeterminedLike {
    def name = this.getClass.getSimpleName.stripSuffix("$")
  }
  // only used by LLA as AMSL anchor
  case object Geodetic extends Alias

  // cast to UAVConf.HomeLocation
  case object Home extends Alias

  // cast to UAV's projection to mean sea level (MSL)
  case object MSLProjection extends Alias

  // cast to UAV's projection to home level
  case object HomeLevelProjection extends Alias

  // cast to UAV's projection to ground terrain
  case object GroundProjection extends Alias

  case class Named(override val name: String) extends Alias
}
