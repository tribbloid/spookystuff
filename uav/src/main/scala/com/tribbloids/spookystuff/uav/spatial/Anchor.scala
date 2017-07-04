package com.tribbloids.spookystuff.uav.spatial

/**
  * Created by peng on 15/02/17.
  */
trait Anchor extends Serializable {

  def getCoordinate(
                     system: CoordinateSystem = LLA,
                     from: Anchor = GeodeticAnchor
                   ): Option[system.V] = {
    _getCoordinate(system, from, InferenceContext())
  }

  def coordinate(
                  system: CoordinateSystem = LLA,
                  from: Anchor = GeodeticAnchor
                ): system.V = {
    getCoordinate(system, from).getOrElse {
      throw new UnsupportedOperationException(s"cannot determine relative position from $from to $this")
    }
  }

  def _getCoordinate(
                      system: CoordinateSystem,
                      from: Anchor = GeodeticAnchor,
                      ic: InferenceContext
                    ): Option[system.V] = None
}

case object GeodeticAnchor extends Anchor {
}
// to be injected by SpookyConf
case object PlaceHoldingAnchor extends Anchor {
}