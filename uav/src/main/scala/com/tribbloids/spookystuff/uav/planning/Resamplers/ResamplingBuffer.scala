package com.tribbloids.spookystuff.uav.planning.Resamplers

import com.tribbloids.spookystuff.uav.actions.{UAVNavigation, Waypoint}
import com.tribbloids.spookystuff.uav.spatial.point.{Location, NED}

import scala.collection.mutable.ArrayBuffer

case class ResamplingBuffer(
                             from: (UAVNavigation#WSchema, (Int, Int)),
                             to: (UAVNavigation#WSchema, (Int, Int)),
                             ts: ArrayBuffer[Double] = ArrayBuffer.empty[Double]
                           ) {

  case class WParams(
                      granularity: Double = 1.0
                    ) {
    val getEffective_ts: Seq[Double] = {
      val filtered = ts.filter {
        v =>
          (v > 0.25) && (v < 0.75)
      }
      if (filtered.isEmpty) Nil
      else
        Seq(filtered.sum / filtered.size)
    }

    val getResult: Seq[Waypoint] = {
      val vectors = getEffective_ts.map {
        t =>
          (1-t) * from._1.vector + t * to._1.vector
      }
      val wps = vectors.map {
        v =>
          val location: Location = NED.fromVec(v) -> to._1.home
          Waypoint(location)
      }
      wps
    }
  }
}
