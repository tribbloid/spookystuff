package com.tribbloids.spookystuff.uav

import org.locationtech.jts.geom.PrecisionModel
import org.locationtech.jts.{geom, io}
import geotrellis.vector.{Geometry, Point}

/**
  * Created by peng on 15/02/17.
  */
package object spatial {

  type JTSCoord = geom.Coordinate
  type JTSGeom = geom.Geometry
  type JTSPoint = geom.Point

  type TrellisGeom = Geometry //wraps jts Geometry
  type TrellisPoint = Point

  type Coordinate = Geom[TrellisPoint]

  /**
    * Should be thread safe??
    */
  object JTSGeomFactory extends geom.GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING_SINGLE))
//  object JTSGeomFactory extends geom.GeometryFactory(new PrecisionModel(0.00000000000001))
  object WKTWriter extends io.WKTWriter(3)
  object WKTReader extends io.WKTReader(JTSGeomFactory)
}
