package com.tribbloids.spookystuff.uav.spatial.point

import com.tribbloids.spookystuff.uav.spatial.Anchor
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory
import geotrellis.proj4.LatLng
import org.osgeo.proj4j.proj.Projection

import scala.language.implicitConversions

/**
  * use WGS84 projection, more will come
  */
object LLA extends CoordinateSystem {

  @transient lazy val projOpt: Option[Projection] = {
    val proj = LatLng.proj4jCrs.getProjection
    Some(proj)
  }

  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  override def get2DProj(a: Anchor, ic: SearchHistory): Option[Projection] = {
    projOpt
  }

  override def _chain(self: LLA.Coordinate, b: LLA.Coordinate) = apply(b.lat, b.lon, self.alt + b.alt)

  case class Proto(
                    lat: Double,
                    lon: Double,
                    alt: Double
                  ) {

    lazy val v: Coordinate = apply(lat, lon, alt)
  }
}