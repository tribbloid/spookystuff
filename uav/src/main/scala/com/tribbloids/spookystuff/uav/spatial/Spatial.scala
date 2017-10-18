package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.uav.spatial.point.CoordinateSystem
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory
import org.apache.spark.mllib.uav.Vec

trait Spatial extends Serializable {

  def system: CoordinateSystem
  def vector: Vec

  var searchHistory: SearchHistory = _
}
