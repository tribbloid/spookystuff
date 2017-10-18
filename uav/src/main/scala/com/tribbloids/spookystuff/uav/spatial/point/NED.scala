package com.tribbloids.spookystuff.uav.spatial.point

import breeze.linalg.DenseVector
import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.spatial.util.{SearchAttempt, SearchHistory}
import org.apache.spark.mllib.uav.Vec
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
        val opt: Option[LLA.C] = ic.getCoordinate(SearchAttempt(Anchors.Geodetic, LLA, p))
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

  override def zeroOpt: Option[C] = Some(create(Vec.zeros(3)))

  override def _fromVector(v: Vec): C = C(v(1), v(0), - v(2))

  def apply(
             north: Double,
             east: Double,
             down: Double
           ) = C(north, east, down)

  case class C(
                north: Double,
                east: Double,
                down: Double
              ) extends this.Coord {

    val vector: DenseVector[Double] = DenseVector(east, north, - down)

    override def _chain(b: C): C = C(this.north + b.north, this.east + b.east, this.down + b.down)
  }
}