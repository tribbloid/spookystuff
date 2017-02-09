package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.{Vector => Vec}
import com.tribbloids.spookystuff.caching.Memoize
import com.tribbloids.spookystuff.utils.{IDMixin, ScalaUDT}
import geotrellis.proj4.LatLng
import org.apache.spark.sql.types.SQLUserDefinedType
import org.osgeo.proj4j.ProjCoordinate
import org.osgeo.proj4j.datum.Ellipsoid
import org.osgeo.proj4j.proj.{EquidistantAzimuthalProjection, Projection}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.{Random, Try}

trait Coordinate extends Serializable {

  def system: CoordinateSystem

  def vector: Vec[Double]

  def x: Double = vector(0)
  def y: Double = vector(1)
  def z: Double = vector(2)

  //implement this to bypass proj4
  def customProjectTo(a: Reference, system2: CoordinateSystem): Option[system2.V] = None

  def projectZ(a: Reference, system2: CoordinateSystem): Option[Double] = Some(z)

  def projectTo(a: Reference, system2: CoordinateSystem): Option[system2.V] = {

    val customResult: Option[system2.V] = customProjectTo(a, system2).map(_.asInstanceOf[system2.V]) // redundant! IDE error?
    customResult.orElse {
      val dstZOpt = projectZ(a, system2)
      for (
        proj1 <- system.get2DProj(a);
        proj2 <- system2.get2DProj(a);
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
  def get2DProj(a: Reference): Option[Projection]

  def create(vector: Vec[Double]): V

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
  override def get2DProj(a: Reference): Option[Projection] = {
    projOpt
  }

  override def create(vector: Vec[Double]): V = V(vector)

  case class V(
                vector: Vec[Double]
              ) extends this.Value {

    def lat = y
    def lon = x
    def alt = z

    override def ++>(b: V): V = V(Vec(b.x, b.y, this.z + b.z))
  }
}

/**
  * use Azimuthal projection (NOT Cartisian but a mere approximation)
  */
object NED extends CoordinateSystem {
  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  override def get2DProj(a: Reference): Option[Projection] = {
    a match {
      case p: Position =>
        val opt =  p.getCoordinate(LLA, GeodeticRef)
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

  override def create(vector: Vec[Double]): V = V(vector)

  case class V(
                vector: Vec[Double]
              ) extends this.Value {

    def north = y
    def east = x
    def down = - z

//    def distance = vector*vector

    override def ++>(b: V): V = V(Vec(this.x + b.x, this.y + b.y, this.z + b.z))
  }
}

trait Reference extends Serializable {
}
case object GeodeticRef extends Reference {
}
//case object GeocentricRef extends Reference {
//}

class PositionUDT() extends ScalaUDT[Position]

// to be enriched by SpookyContext (e.g. baseLocation, CRS etc.)
@SQLUserDefinedType(udt = classOf[PositionUDT])
@SerialVersionUID(-928750192836509428L)
case class Position(
                     id: Long = Random.nextLong(),
                     coordinates: Seq[(Coordinate, Reference)] = Nil
                   ) extends Reference with IDMixin {

  val buffer: ArrayBuffer[(Coordinate, Reference)] = ArrayBuffer.empty

  // always add result into buffer to avoid repeated computation
  // recursively search through its relations to deduce the coordinate.
  def getCoordinate(system: CoordinateSystem, from: Reference = GeodeticRef): Option[system.V] = {

    val allCoordinates  = coordinates ++ buffer
    val existing = allCoordinates.find(_._1.system == system)
    if (existing.nonEmpty) return existing.map(_._1.asInstanceOf[system.V])

    allCoordinates.foreach {
      tuple =>
        val result: Option[CoordinateSystem#V] = tuple._1.projectTo(from, system)
        if (result.nonEmpty) {
          buffer += result.get -> from
          return result.map(_.asInstanceOf[system.V])
        }
    }
    None
  }

  // geodesic distance, not always cartesian distance.
  // T refers to the type of the CRS that defines the manifold
  //  def d[T <: CoordinateLike](b: Position): Try[Double] = {
  //    Position.d(this, b)
  //  }

  //  def samePosition(b: Position) = {
  //    (this.id == b.id) ||
  //      (this.d(b) == Success(0))
  //  }

  def _id = id
}

object Position {

  val dMat = {
    Memoize[(Position, Position, ClassTag[_]), Try[Double]] {
      triplet =>
        val (a, b, c) = triplet
        ???
    }
  }

  //  def d[T <: CoordinateLike : ClassTag](a: Position, b: Position): Try[Double] = {
  //    val ctg = implicitly[ClassTag[T]]
  //    dMat((a, b, ctg))
  //  }
}