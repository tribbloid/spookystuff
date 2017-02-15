package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.{Vector => Vec}
import geotrellis.proj4.LatLng
import org.osgeo.proj4j.ProjCoordinate
import org.osgeo.proj4j.datum.Ellipsoid
import org.osgeo.proj4j.proj.{EquidistantAzimuthalProjection, Projection}

import scala.language.implicitConversions

trait Coordinate extends Serializable {

  def system: CoordinateSystem

  def vector: Vec[Double]

  def x: Double = vector(0)
  def y: Double = vector(1)
  def z: Double = vector(2)

  //implement this to bypass proj4
  def fastProjectTo(ref1: Anchor, ref2: Anchor, system2: CoordinateSystem, cyclic: Set[Anchor]): Option[system2.V] = {
    system2 match {
      case NED if ref1 == ref2 => Some(NED.V(0,0,0).asInstanceOf[system2.V])
      case _ => None
    }
    //    None
  }

  def projectZ(ref1: Anchor, ref2: Anchor, system2: CoordinateSystem, cyclic: Set[Anchor]): Option[Double] = {

    val delta2_1Opt = ref1._getCoordinate(LLA, ref2, cyclic)
      .map(v => v.alt)
      .orElse {
        ref2._getCoordinate(LLA, ref1, cyclic)
          .map(v => - v.alt)
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
  def projectTo(ref1: Anchor, ref2: Anchor, system2: CoordinateSystem, cyclic: Set[Anchor]): Option[system2.V] = {

    val customResult: Option[system2.V] = fastProjectTo(ref1, ref2, system2, cyclic).map(_.asInstanceOf[system2.V]) // redundant! IDE error?
    customResult.orElse {
      val dstZOpt = projectZ(ref1, ref2, system2, cyclic)
      for (
        proj1 <- system.get2DProj(ref2, cyclic);
        proj2 <- system2.get2DProj(ref2, cyclic);
        dstZ <- dstZOpt
      ) yield {
        val src = new ProjCoordinate(x, y)
        val wgs84 = new ProjCoordinate
        val dst = new ProjCoordinate
        proj1.inverseProject(src, wgs84)
        proj2.project(wgs84, dst)
        system2.create(Vec(dst.x, dst.y, dstZ))
      }
    }
  }
}

/**
  * represents a mapping from 1 position or reference to another position given a CRS
  * subclasses MUST define a CRS or worthless
  */
trait CoordinateSystem extends Serializable {
  outer =>

  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  def get2DProj(a: Anchor, cyclic: Set[Anchor]): Option[Projection]

  protected def _create(vector: Vec[Double]): V
  def create(vector: Vec[Double]) = {
    val result = _create(vector)
    assert(result.vector == vector)
    result
  }

  type V <: Value

  trait Value extends Coordinate {
    def system: CoordinateSystem = CoordinateSystem.this

    def ++>(b: V): V
  }
}

/**
  * use WGS84 projection, more will come
  */
object LLA extends CoordinateSystem {

  @transient lazy val projOpt = {
    val proj = LatLng.proj4jCrs.getProjection
    Some(proj)
  }

  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  override def get2DProj(a: Anchor, cyclic: Set[Anchor]): Option[Projection] = {
    projOpt
  }

  def apply(
             lat: Double,
             lon: Double,
             alt: Double
           ) = V(lat, lon, alt)

  override def _create(v: Vec[Double]): V = V(v(1), v(0), v(2))

  case class V(
                lat: Double,
                lon: Double,
                alt: Double
              ) extends this.Value {

    val vector = Vec(lon, lat, alt)

    override def ++>(b: V): V = V(b.lat, b.lon, this.alt + b.alt)

    override lazy val toString = s"${this.system.getClass.getSimpleName.stripSuffix("$")} lat=$lat lon=$lon alt=$alt"
  }
}

/**
  * use Azimuthal projection (NOT Cartisian but a mere approximation)
  */
object NED extends CoordinateSystem {
  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  override def get2DProj(a: Anchor, cyclic: Set[Anchor]): Option[Projection] = {
    a match {
      case p: Location =>
        val opt =  p._getCoordinate(LLA, GeodeticAnchor, cyclic)
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

  override def _create(v: Vec[Double]): V = V(v(1), v(0), - v(2))

  def apply(
             north: Double,
             east: Double,
             down: Double
           ) = V(north, east,- down)

  case class V(
                north: Double,
                east: Double,
                down: Double
              ) extends this.Value {

    val vector = Vec(east, north, - down)

    override def ++>(b: V): V = V(this.north + b.north, this.east + b.east, this.down + b.down)

    override lazy val toString = s"${this.system.getClass.getSimpleName.stripSuffix("$")} north=$north east=$east down=$down"
  }
}