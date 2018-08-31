package org.apache.spark.ml.uav

import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.spatial.point.NED

object TwoLines {

  def apply(
      A1: UAVNavigation#WSchema,
      B1: UAVNavigation#WSchema,
      A2: UAVNavigation#WSchema,
      B2: UAVNavigation#WSchema
  ): TwoLines =
    TwoLines(A1.coordinate, B1.coordinate, A2.coordinate, B2.coordinate)
}

case class TwoLines(
    A1: NED.Coordinate,
    B1: NED.Coordinate,
    A2: NED.Coordinate,
    B2: NED.Coordinate
) {

  val (t1, t2) = {
    val M = A1.vector - A2.vector
    val C1 = B1.vector - A1.vector
    val C2 = B2.vector - A2.vector

    val CC1 = C1.t * C1
    val CC2 = C2.t * C2

    def clamp(_t1: Double, _t2: Double) = {
      val t1 = Math.max(Math.min(1.0, _t1), 0.0)
      val t2 = Math.max(Math.min(1.0, _t2), 0.0)
      t1 -> t2
    }

    (CC1, CC2) match {
      case (0.0, 0.0) =>
        (0.0, 0.0)
      case (0.0, _) =>
        val t1 = 0
        val t2 = C2.t * M / CC2
        clamp(t1, t2)
      case (_, 0.0) =>
        val t2 = 0
        val t1 = -C1.t * M / CC1
        clamp(t1, t2)
      case _ =>
        val C21 = C2 * C1.t
        val G = C21 - C21.t
        val C1TGC2 = C1.t * G * C2

        if (C1TGC2 == 0) {
          (0.0, 0.0)
        } else {
          def t1 = -(M.t * G * C2) / C1TGC2
          def t2 = -(M.t * G * C1) / C1TGC2

          clamp(t1, t2)
        }
    }
  }

  val M = A1.vector - A2.vector
  val C1 = B1.vector - A1.vector
  val C2 = B2.vector - A2.vector

  val P: Vec = M + t1 * C1 - t2 * C2
  val DSquare = P dot P
  val D = Math.sqrt(DSquare)
}
