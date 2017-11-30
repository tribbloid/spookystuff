package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.uav.spatial.point.CoordinateSystem

/**
  * has information (can be contradicting or probabilistic) from
  * different sensors or estimators fused together
  * @tparam T
  */
trait GeoFusion[T <: Spatial[_]] {

  def definedBy: Seq[GeoRef[T]]

  def reanchor(anchor: Anchor, system: CoordinateSystem): Option[this.type]
}
