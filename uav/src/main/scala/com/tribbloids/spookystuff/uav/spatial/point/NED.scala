package com.tribbloids.spookystuff.uav.spatial.point

import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.spatial.util.{SearchAttempt, SearchHistory}
import org.osgeo.proj4j.datum.Ellipsoid
import org.osgeo.proj4j.proj.{EquidistantAzimuthalProjection, Projection}

import scala.language.implicitConversions

/**
  * use Azimuthal projection (NOT Cartisian but a mere approximation)
  */
object NED extends CoordinateSystem {

  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  override def get2DProj(a: Anchor, ic: SearchHistory): Option[Projection] = {
    a match {
      case p: Location =>
        val opt: Option[LLA.Coordinate] = ic.getCoordinate(SearchAttempt(Anchors.Geodetic, LLA, p))
        opt.map {
          origin =>
            val proj = new EquidistantAzimuthalProjection(Math.toRadians(origin.lat), Math.toRadians(origin.lon))
            proj.setEllipsoid(Ellipsoid.WGS84)
            proj.initialize()
            proj
        }
      case _ =>
        None
    }
  }

  override def _chain(self: Coordinate, b: Coordinate): Coordinate = {
    apply(self.north + b.north, self.east + b.east, self.down + b.down)
  }

  case class Proto(
                    north: Double,
                    east: Double,
                    down: Double
                  ) {

    lazy val v: Coordinate = apply(north, east, down)
  }
}