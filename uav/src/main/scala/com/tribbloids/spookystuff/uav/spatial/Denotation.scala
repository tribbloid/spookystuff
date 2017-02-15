package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.{Vector => Vec}
import org.osgeo.proj4j.ProjCoordinate

/**
  * Created by peng on 15/02/17.
  */
case class Denotation(
                      coordinate: Coordinate,
                      anchor: Anchor
                    ) {

  /**
    * @param ref2 origin of the new coordinate system
    * @param system2 type of the new coordinate system
    * @return new coordinate
    */
  def projectTo(ref2: Anchor, system2: CoordinateSystem, cyclic: Set[Anchor]): Option[system2.V] = {
    coordinate.projectTo(anchor, ref2, system2, cyclic).map(_.asInstanceOf[system2.V])
  }
}
