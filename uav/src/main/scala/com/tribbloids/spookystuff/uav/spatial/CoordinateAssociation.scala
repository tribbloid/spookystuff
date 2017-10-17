package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory

import scala.language.implicitConversions

object CoordinateAssociation {

  implicit def fromTuple(tuple: (Coordinate, Anchor)) = CoordinateAssociation(tuple._1, tuple._2)
}

/**
  * Created by peng on 15/02/17.
  * anchored coordinate
  */
case class CoordinateAssociation(
                                  data: Coordinate,
                                  anchor: Anchor
                                ) extends Association {

  /**
    * @param from origin of the new coordinate system
    * @param system type of the new coordinate system
    * @return new coordinate
    */
  def project(from: Anchor, system: SpatialSystem, sh: SearchHistory): Option[system.C] = {
    data.project(anchor, from, system, sh)//.map(_.asInstanceOf[system2.V])
  }
}
