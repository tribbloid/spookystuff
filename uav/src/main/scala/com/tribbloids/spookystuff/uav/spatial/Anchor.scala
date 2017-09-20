package com.tribbloids.spookystuff.uav.spatial

/**
  * Created by peng on 15/02/17.
  */
trait Anchor extends Serializable {
  import Anchors._

  def getCoordinate(
                     system: CoordinateSystem = LLA,
                     from: Anchor = Geodetic
                   ): Option[system.V] = {
    _getCoordinate(system, from, SearchHistory())
  }

  def coordinate(
                  system: CoordinateSystem = LLA,
                  from: Anchor = Geodetic
                ): system.V = {
    getCoordinate(system, from).getOrElse {
      throw new UnsupportedOperationException(
        s"cannot determine relative position from $from to $this"
      )
    }
  }

  def _getCoordinate(
                      system: CoordinateSystem,
                      from: Anchor = Geodetic,
                      ic: SearchHistory
                    ): Option[system.V] = None
}

