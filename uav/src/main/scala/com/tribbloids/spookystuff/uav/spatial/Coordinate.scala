package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.{DenseVector, Vector => Vec}
import com.tribbloids.spookystuff.uav.spatial.util.{SearchHistory, SearchAttempt}
import com.tribbloids.spookystuff.utils.ReflectionUtils
import geotrellis.proj4.LatLng
import org.osgeo.proj4j.ProjCoordinate
import org.osgeo.proj4j.datum.Ellipsoid
import org.osgeo.proj4j.proj.{EquidistantAzimuthalProjection, Projection}

import scala.language.implicitConversions

trait Coordinate extends SpatialData {

  def system: SpatialSystem

  def vector: Vec[Double]

  def x: Double = vector(0)
  def y: Double = vector(1)
  def z: Double = vector(2)

  var searchHistory: SearchHistory = _

  //implement this to bypass proj4
  def fastProjectTo(
                     ref1: Anchor,
                     ref2: Anchor, system2: SpatialSystem,
                     ic: SearchHistory
                   ): Option[system2.C] = {
    system2 match {
      case NED if ref1 == ref2 => Some(NED.C(0,0,0).asInstanceOf[system2.C])
      case _ => None
    }
  }

  def projectZ(ref1: Anchor, ref2: Anchor, system2: SpatialSystem, ic: SearchHistory): Option[Double] = {

    val delta2_1Opt = {
      ic.getCoordinate(SearchAttempt(ref2, LLA, ref1))
        .map(_.alt)
        .orElse {
          ic.getCoordinate(SearchAttempt(ref1, LLA, ref2))
            .map(v => - v.alt)
        }
    }

    delta2_1Opt.map {
      delta =>
        delta + z
    }
  }

  /**
    * @param ref2 origin of the new coordinate system
    * @param system2 type of the new coordinate system
    * @return new coordinate
    */
  def project(
               ref1: Anchor,
               ref2: Anchor,
               system2: SpatialSystem,
               ic: SearchHistory
             ): Option[system2.C] = {

    val customResult: Option[system2.C] = fastProjectTo(ref1, ref2, system2, ic)
      .map(_.asInstanceOf[system2.C]) // redundant! IDE error?
    customResult.orElse {
      val dstZOpt = projectZ(ref1, ref2, system2, ic)
      for (
        proj1 <- system.get2DProj(ref1, ic);
        proj2 <- system2.get2DProj(ref2, ic);
        dstZ <- dstZOpt
      ) yield {
        val src = new ProjCoordinate(x, y)
        val wgs84 = new ProjCoordinate
        val dst = new ProjCoordinate
        proj1.inverseProject(src, wgs84)
        proj2.project(wgs84, dst)
        system2.create(Vec(dst.x, dst.y, dstZ), ic)
      }
    }
  }

  override lazy val toString = {
    s"${this.system.name} " + {
      this match {
        case v: Product =>
          ReflectionUtils.getCaseAccessorMap(v).map {
            case (vv, d: Double) => s"$vv=${d.formatted("%f")}"
            case (vv, d@ _) => s"$vv=$d"
          }
            .mkString(" ")
        case _ => super.toString
      }
    }
  }

  def toStrWithInferenceCtx = (Seq(toString) ++ Option(searchHistory).toSeq).mkString(" ")
}


/**
  * use WGS84 projection, more will come
  */
object LLA extends SpatialSystem {

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

  override def _fromVector(v: Vec[Double]): C = C(v(1), v(0), v(2))

  case class C(
                lat: Double,
                lon: Double,
                alt: Double
              ) extends this.Coord {

    val vector: DenseVector[Double] = DenseVector(lon, lat, alt)

    override def _chain(b: C): C = C(b.lat, b.lon, this.alt + b.alt)
  }
}

/**
  * use Azimuthal projection (NOT Cartisian but a mere approximation)
  */
object NED extends SpatialSystem {
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

  override def _fromVector(v: Vec[Double]): C = C(v(1), v(0), - v(2))

  override def zero: Option[C] = Some(create(Vec(0.0, 0.0, 0.0)))

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