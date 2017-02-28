package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.{Vector => Vec}
import com.tribbloids.spookystuff.utils.SpookyUtils
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

  var ic: InferenceContext = _

  //implement this to bypass proj4
  def fastProjectTo(ref1: Anchor, ref2: Anchor, system2: CoordinateSystem, ic: InferenceContext): Option[system2.V] = {
    system2 match {
      case NED if ref1 == ref2 => Some(NED.V(0,0,0).asInstanceOf[system2.V])
      case _ => None
    }
    //    None
  }

  def projectZ(ref1: Anchor, ref2: Anchor, system2: CoordinateSystem, ic: InferenceContext): Option[Double] = {

    val delta2_1Opt = {
      ic.getCoordinate(PendingTriplet(ref2, LLA, ref1))
        .map(_.asInstanceOf[LLA.V].alt)
        .orElse {
          ic.getCoordinate(PendingTriplet(ref1, LLA, ref2))
            .map(v => - v.asInstanceOf[LLA.V].alt)
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
  def project(ref1: Anchor, ref2: Anchor, system2: CoordinateSystem, ic: InferenceContext): Option[system2.V] = {

    val customResult: Option[system2.V] = fastProjectTo(ref1, ref2, system2, ic)
      .map(_.asInstanceOf[system2.V]) // redundant! IDE error?
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
          SpookyUtils.Reflection.getCaseAccessorMap(v).map {
            case (vv, d: Double) => s"$vv=${d.formatted("%f")}"
            case (vv, d@ _) => s"$vv=$d"
          }
            .mkString(" ")
        case _ => super.toString
      }
    }
  }

  def withICString = (Seq(toString) ++ Option(ic).toSeq).mkString(" ")
}

/**
  * represents a mapping from 1 position or reference to another position given a CRS
  * subclasses MUST define a CRS or worthless
  */
trait CoordinateSystem extends Serializable {

  def name: String = this.getClass.getSimpleName.stripSuffix("$")

  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  def get2DProj(a: Anchor, ic: InferenceContext): Option[Projection]

  protected def _create(vector: Vec[Double]): V
  def create(vector: Vec[Double], ic: InferenceContext = InferenceContext()) = {
    val result = _create(vector)
    assert(result.vector == vector) //TODO: remove
    result.ic = ic
    result
  }

  type V <: Value

  def zero: Option[V] = None

  trait Value extends Coordinate {
    def system: CoordinateSystem = CoordinateSystem.this

    final def ++>(b: V): V = {
      val result = _chain(b)
      assert(this.ic == b.ic)
      result.ic = this.ic
      result
    }

    protected def _chain(b: V): V
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
  override def get2DProj(a: Anchor, ic: InferenceContext): Option[Projection] = {
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

    override def _chain(b: V): V = V(b.lat, b.lon, this.alt + b.alt)
  }
}

/**
  * use Azimuthal projection (NOT Cartisian but a mere approximation)
  */
object NED extends CoordinateSystem {
  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  override def get2DProj(a: Anchor, ic: InferenceContext): Option[Projection] = {
    a match {
      case p: Location =>
        val opt: Option[LLA.V] = ic.getCoordinate(PendingTriplet(GeodeticAnchor, LLA, p)).map(_.asInstanceOf[LLA.V])
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

  override def zero: Option[V] = Some(create(Vec(0.0, 0.0, 0.0)))

  def apply(
             north: Double,
             east: Double,
             down: Double
           ) = V(north, east, down)

  case class V(
                north: Double,
                east: Double,
                down: Double
              ) extends this.Value {

    val vector = Vec(east, north, - down)

    override def _chain(b: V): V = V(this.north + b.north, this.east + b.east, this.down + b.down)
  }
}