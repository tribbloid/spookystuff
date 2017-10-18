package com.tribbloids.spookystuff.uav.spatial.point

import com.tribbloids.spookystuff.uav.spatial.Anchor
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory
import org.apache.spark.mllib.uav.Vec
import org.osgeo.proj4j.proj.Projection

import scala.language.implicitConversions

/**
  * represents a mapping from 1 position or reference to another position given a CRS
  * subclasses MUST define a CRS or worthless
  */
trait CoordinateSystem extends Serializable {

  def name: String = this.getClass.getSimpleName.stripSuffix("$")

  //to save time we avoid using proj4 string parsing and implement our own alternative conversion rule if Projection is not available.
  def get2DProj(a: Anchor, ic: SearchHistory): Option[Projection]

  protected def _fromVector(vector: Vec): C
  def create(vector: Vec, ic: SearchHistory = SearchHistory()) = {
    val result = _fromVector(vector)
    assert(result.vector == vector) //TODO: remove
    result.searchHistory = ic
    result
  }

  def zeroOpt: Option[C] = None

  type C <: Coord
  trait Coord extends Coordinate {
    def system: CoordinateSystem = CoordinateSystem.this

    /**
      * NOT commutative
      * @param b
      * @return
      */
    final def :+(b: C): C = {
      val result = _chain(b)
      assert(this.searchHistory == b.searchHistory)
      result.searchHistory = this.searchHistory
      result
    }

    protected def _chain(b: C): C
  }
}
