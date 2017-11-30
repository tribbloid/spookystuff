package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.spatial.point.{Location, NED}
import com.tribbloids.spookystuff.uav.spatial.Anchors

/**
  * Created by peng on 24/02/17.
  */
object UAVTestUtils {

  abstract class Pattern {

    def neds: Seq[(NED.Coordinate, NED.Coordinate)]

    val locations = neds.map {
      tuple =>
        val l1: Location = tuple._1
        val l2: Location = tuple._2
        l1.replaceAnchors{
          case Anchors.Home =>
            UAVConf.DEFAULT_HOME_LOCATION
        } -> l2.replaceAnchors{
          case Anchors.Home =>
            UAVConf.DEFAULT_HOME_LOCATION
        }
    }

    val lineScans: Seq[List[Waypoint]] = {
      locations.map {
        tuple =>
          List(
            Waypoint(Lit(tuple._1)),
            Waypoint(Lit(tuple._2))
          )
      }
    }

    val waypoints: Seq[Waypoint] = {
      lineScans.flatten
    }
  }

  case class NEDPattern(
                         neds: Seq[(NED.Coordinate, NED.Coordinate)]
                       ) extends Pattern {

  }

  case class LawnMowerPattern(
                               n: Int,
                               origin: NED.Coordinate,
                               dir: NED.Coordinate, // actual directions are always alternating
                               stride: NED.Coordinate
                             ) extends Pattern {

    def neds: Seq[(NED.Coordinate, NED.Coordinate)] = {

      val result = (0 until n).map {
        i =>
          val p1: NED.Coordinate = NED.vec2Coordinate(origin.vector + (stride.vector :* i.toDouble))
          val p2: NED.Coordinate = NED.vec2Coordinate(p1.vector + dir.vector)
          if (i % 2 == 0) {
            p1 -> p2
          }
          else {
            p2 -> p1
          }
      }
      result
    }
  }
}
