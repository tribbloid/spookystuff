package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.uav.spatial.point.{CoordinateSystem, LLA}
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory

/**
  * Created by peng on 15/02/17.
  * An Anchor maps a data structure (e.g. coordinate, voxel map, point cloud etc.)
  * to a real world space.
  * An Anchor can be a location, a location with bearing,
  * an estimation of a location with PDF, or a completely unknown hypothesis
  */
trait Anchor extends Serializable {
  import Anchors._

  def name: String

  final def getCoordinate(
                           system: CoordinateSystem = LLA,
                           from: Anchor = Geodetic
                         ): Option[system.C] = {
    _getCoordinate(system, from, SearchHistory())
  }

  final def coordinate(
                        system: CoordinateSystem = LLA,
                        from: Anchor = Geodetic
                      ): system.C = {

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
                    ): Option[system.C] = None
}

trait LocationLike extends Anchor {
}
trait EstimationLike extends Anchor {
}
trait UndeterminedLike extends Anchor {
}
