package com.tribbloids.spookystuff.uav.spatial.point

import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory
import org.apache.spark.ml.uav.Vec
import org.osgeo.proj4j.proj.Projection

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * represents a mapping from 1 position or reference to another position given a CRS
  * subclasses MUST define a CRS or worthless
  * NOT a CRS: Coordinate Reference System (CRS) definitions as coordinate systems related to the earth through datum.
  * Coordinate System (CS) definitions as the set of coordinate system axes that spans the coordinate space.
  */
trait CoordinateSystem extends Serializable {

  def fromTrellis[T <: TrellisGeom: ClassTag](v: T) = {
    val result = new CSGeom(v)
    result
  }
  def fromWKT(wkt: String) = {
    val parsed: JTSGeom = WKTReader.read(wkt)
    fromTrellis[TrellisGeom](parsed)
  }

  lazy val name: String = this.getClass.getSimpleName.stripSuffix("$")

  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  def get2DProj(a: Anchor, ic: SearchHistory): Option[Projection]

  //TODO: this is unmathical, should be superceded by reverse operator
  def zeroOpt: Option[Coordinate] = None

  //  def fromXYZ(x: Double, y: Double, z: Double, existingOpt: Option[TrellisPoint] = None): TrellisPoint = {
  //    val point: TrellisPoint = existingOpt.getOrElse {
  //      JTSGeomFactory.createPoint(new JTSCoord(x, y, z))
  //    }
  //    point
  //  }

  def fromVec(vector: Vec, sh: SearchHistory = SearchHistory()) = {
    val result = fromXYZ(vector(0), vector(1), vector(2), sh)
    assert(result.vector == vector) //TODO: remove
    result
  }
  def fromXYZ(
      x: Double,
      y: Double,
      z: Double,
      sh: SearchHistory = SearchHistory()
  ) = {
    val result = new CoordinateRepr(x, y, z)
    result.searchHistory = sh
    result
  }

  def _chain(self: Coordinate, b: Coordinate): Coordinate

  class CSGeom[+T <: TrellisGeom: ClassTag](override val trellisGeom: T) extends Geom[T] {

    def system = CoordinateSystem.this
  }
  type Coordinate = CSGeom[TrellisPoint]
  class CoordinateRepr(
      x: Double,
      y: Double,
      z: Double
  ) extends Coordinate(JTSGeomFactory.createPoint(new JTSCoord(x, y, z))) {}
  type Repr <: CoordinateRepr
  def toRepr(v: Coordinate): Repr

  object CSGeom {

    implicit class CoordinateView(val self: Coordinate) extends PointViewBase {

      /**
        * NOT commutative
        * @param b
        * @return
        */
      final def :+(b: Coordinate): Coordinate = {
        val result = CoordinateSystem.this._chain(self, b)
        assert(self.searchHistory == b.searchHistory)
        result.searchHistory = self.searchHistory
        result
      }
    }

    implicit def toRepr(v: Coordinate): Repr = CoordinateSystem.this.toRepr(v)
  }
}
