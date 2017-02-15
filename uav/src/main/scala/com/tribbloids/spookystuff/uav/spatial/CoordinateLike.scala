package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.{Vector => Vec}
import com.tribbloids.spookystuff.caching.Memoize
import com.tribbloids.spookystuff.utils.ScalaUDT
import geotrellis.proj4.LatLng
import org.apache.spark.sql.types.SQLUserDefinedType
import org.osgeo.proj4j.ProjCoordinate
import org.osgeo.proj4j.datum.Ellipsoid
import org.osgeo.proj4j.proj.{EquidistantAzimuthalProjection, Projection}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Try

trait Coordinate extends Serializable {

  def system: CoordinateSystem

  def vector: Vec[Double]

  def x: Double = vector(0)
  def y: Double = vector(1)
  def z: Double = vector(2)

  //implement this to bypass proj4
  def fastProjectTo(ref1: Reference, ref2: Reference, system2: CoordinateSystem): Option[system2.V] = None

  def projectZ(ref1: Reference, ref2: Reference, system2: CoordinateSystem): Option[Double] = {

    val delta2_1Opt = ref1.getCoordinate(LLA, ref2)
      .map(v => v.alt)
      .orElse {
        ref2.getCoordinate(LLA, ref1)
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
  def projectTo(ref1: Reference, ref2: Reference, system2: CoordinateSystem): Option[system2.V] = {

    val customResult: Option[system2.V] = fastProjectTo(ref1, ref2, system2).map(_.asInstanceOf[system2.V]) // redundant! IDE error?
    customResult.orElse {
      val dstZOpt = projectZ(ref1, ref2, system2)
      for (
        proj1 <- system.get2DProj(ref2);
        proj2 <- system2.get2DProj(ref2);
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

    override lazy val toString = s"${this.system.getClass.getSimpleName.stripSuffix("$")} lat=$lat lon=$lon alt=$alt"
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

    override lazy val toString = s"${this.system.getClass.getSimpleName.stripSuffix("$")} north=$north east=$east down=$down"
  }
}

trait Reference extends Serializable {

  def getCoordinate(system: CoordinateSystem = LLA, from: Reference = GeodeticRef): Option[system.V]
}
case object GeodeticRef extends Reference {
  override def getCoordinate(system: CoordinateSystem, from: Reference): Option[system.V] = {
    if (from == this) Some(system.create(Vec(0,0,0)))
    else None
  }
}

class PositionUDT() extends ScalaUDT[Position]

// to be enriched by SpookyContext (e.g. baseLocation, CRS etc.)
@SQLUserDefinedType(udt = classOf[PositionUDT])
@SerialVersionUID(-928750192836509428L)
case class Position(
                     //                     id: Long = Random.nextLong(),
                     coordinates: Seq[(Coordinate, Reference)] = Nil
                   ) extends Reference {

  val buffer: ArrayBuffer[(Coordinate, Reference)] = ArrayBuffer.empty

  // always add result into buffer to avoid repeated computation
  // recursively search through its relations to deduce the coordinate.
  //TODO: need testing
  def getCoordinate(system: CoordinateSystem = LLA, from: Reference = GeodeticRef): Option[system.V] = {

    val allCoordinates  = coordinates ++ buffer
    val exactMatch = allCoordinates.find(v => v._1.system == system && v._2 == from)
    exactMatch.foreach {
      tuple =>
        return Some(tuple._1.asInstanceOf[system.V])
    }

    def cacheAndBox(v: Coordinate): Option[system.V] = {
      buffer += v -> from
      Some(v.asInstanceOf[system.V])
    }

    allCoordinates.foreach {
      tuple =>
        val directOpt: Option[CoordinateSystem#V] = tuple._1.projectTo(tuple._2, from, system)
        directOpt.foreach {
          direct =>
            return cacheAndBox(direct)
        }

        //use chain rule for inference
        tuple._2 match {
          case p: Position =>
            val c1Opt: Option[system.V] = p.getCoordinate(system, from)
              .map(_.asInstanceOf[system.V])
            val c2Opt: Option[system.V] = this.getCoordinate(system, p)
              .map(_.asInstanceOf[system.V])
            for (
              c1 <- c1Opt;
              c2 <- c2Opt
            ) {
              return cacheAndBox(c1 ++> c2)
            }
          case _ =>
        }
    }

    //add reverse deduction.

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