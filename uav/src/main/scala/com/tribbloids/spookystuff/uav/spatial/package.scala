package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.uav.spatial.point.CoordinateSystem
import com.vividsolutions.jts
import geotrellis.vector.{Geometry, Point}

/**
  * Created by peng on 15/02/17.
  */
package object spatial {

  type JTSCoord = jts.geom.Coordinate
  type JTSGeom = jts.geom.Geometry
  type JTSPoint = jts.geom.Point

  type TrellisGeom = Geometry //wraps jts Geometry
  type TrellisPoint = Point

  type Coordinate = CoordinateSystem#Coordinate
}
