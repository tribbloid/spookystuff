package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.DenseVector
import com.tribbloids.spookystuff.uav.spatial.point.CoordinateSystem
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory

trait Spatial extends Serializable {

  def system: CoordinateSystem
  def vector: DenseVector[Double]

  @volatile var _searchHistory: SearchHistory = _
  def searchHistory = Option(_searchHistory).getOrElse {
    _searchHistory = SearchHistory()
    _searchHistory
  }

  def searchHistory_=(v: SearchHistory): Unit = {
    _searchHistory = v
  }
}