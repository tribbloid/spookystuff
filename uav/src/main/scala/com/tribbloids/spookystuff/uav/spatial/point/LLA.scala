package com.tribbloids.spookystuff.uav.spatial.point

import breeze.linalg.DenseVector
import com.tribbloids.spookystuff.uav.spatial.Anchor
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory
import geotrellis.proj4.LatLng
import org.apache.spark.mllib.uav.Vec
import org.osgeo.proj4j.proj.Projection

import scala.language.implicitConversions

/**
  * use WGS84 projection, more will come
  */
object LLA extends CoordinateSystem {

  @transient lazy val projOpt = {
    val proj = LatLng.proj4jCrs.getProjection
    Some(proj)
  }

  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  override def get2DProj(a: Anchor, ic: SearchHistory): Option[Projection] = {
    projOpt
  }

  def apply(
             lat: Double,
             lon: Double,
             alt: Double
           ) = C(lat, lon, alt)

  override def _fromVector(v: Vec): C = C(v(1), v(0), v(2))

  case class C(
                lat: Double,
                lon: Double,
                alt: Double
              ) extends this.Coord {

    val vector: DenseVector[Double] = DenseVector(lon, lat, alt)

    override def _chain(b: C): C = C(b.lat, b.lon, this.alt + b.alt)
  }
}