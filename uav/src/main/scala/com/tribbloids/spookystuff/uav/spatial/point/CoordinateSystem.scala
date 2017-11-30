package com.tribbloids.spookystuff.uav.spatial.point

import com.tribbloids.spookystuff.uav.spatial._
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory
import org.apache.spark.mllib.uav.Vec
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

  def _fromTrellis[T <: TrellisGeom: ClassTag](v: T) = new CRSSpatial(v)
  final def fromTrellis[T <: TrellisGeom: ClassTag](v: T, ic: SearchHistory = SearchHistory()) = {
    val result = _fromTrellis(v)
    result.searchHistory = ic
    result
  }

  def name: String = this.getClass.getSimpleName.stripSuffix("$")

  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  def get2DProj(a: Anchor, ic: SearchHistory): Option[Projection]

  def zeroOpt: Option[CRSSpatial[TrellisPoint]] = Some(apply(0, 0, 0))

  //  trait Coordinate extends AbstractSpatial[TrellisPoint] with CoordinateLike {
  //    /**
  //      * NOT commutative
  //      * @param b
  //      * @return

  //      */
  //    final def :+(b: C): C = {
  //      val result = _chain(b)
  //      assert(this.searchHistory == b.searchHistory)
  //      result.searchHistory = this.searchHistory
  //      result
  //    }
  //
  //    protected def _chain(b: C): C
  //  }

  type Coordinate = this.CRSSpatial[TrellisPoint]

  def apply(
             x: Double,
             y: Double,
             z: Double
           ): Coordinate = {

    val jtsPoint = JTSGeomFactory.createPoint(new JTSCoord(x, y, z))
    new CRSSpatial[TrellisPoint](jtsPoint).asInstanceOf[Coordinate]
  }

  protected def _fromVec(vector: Vec): Coordinate = CoordinateSystem.this.apply(vector(0), vector(1), vector(2))

  final def fromVec(vector: Vec, ic: SearchHistory = SearchHistory()) = {
    val result = _fromVec(vector)
    assert(result.vector == vector) //TODO: remove
    result.searchHistory = ic
    result
  }

  def _chain(self: Coordinate, b: Coordinate): Coordinate

  class CRSSpatial[+T <: TrellisGeom: ClassTag](override val geom: T) extends GeomSpatial[T] {

    def system = CoordinateSystem.this

    override lazy val toString = {
      s"${system.name} ${geom.toString}" //TODO: use WKT (well known text)?
      //      + {
      //        this match {
      //          case v: Product =>
      //            ReflectionUtils.getCaseAccessorMap(v).map {
      //              case (vv, d: Double) => s"$vv=${d.formatted("%f")}"
      //              case (vv, d@ _) => s"$vv=$d"
      //            }
      //              .mkString(" ")
      //          case _ => super.toString
      //        }
      //      }
    }

    def toStr_withSearchHistory = (Seq(toString) ++ Option(searchHistory).toSeq).mkString(" ")
  }

  object CRSSpatial {

    implicit class CoordinateView(override val self: Coordinate) extends
      Spatial.PointView(self) {

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

    //TODO: this is not cool but so far no good circumvention
    implicit def llaProto(self: LLA.Coordinate): LLA.Proto = {
      LLA.Proto(self.x, self.y, self.z)
    }
    implicit def nedProto(self: NED.Coordinate): NED.Proto = {
      NED.Proto(self.x, self.y, self.z)
    }
  }
}
