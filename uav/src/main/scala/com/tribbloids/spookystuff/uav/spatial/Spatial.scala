package com.tribbloids.spookystuff.uav.spatial

import breeze.linalg.DenseVector
import com.tribbloids.spookystuff.uav.spatial.point.{CoordinateSystem, LLA, NED}
import com.tribbloids.spookystuff.uav.spatial.util.SearchHistory

import scala.util.Try

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

object Spatial {

  def parse(s: String): Spatial = {
    val splitted = s.split(':')
    val cs = splitted.head
    val wkt = splitted.last
    val geomTry = Try {
      cs match {
        case LLA.name => LLA.fromWKT(wkt)
        case NED.name => NED.fromWKT(wkt)
      }
    }
    geomTry.getOrElse {
      ???
      //TODO: parse non-geometric spatial
    }
  }
}