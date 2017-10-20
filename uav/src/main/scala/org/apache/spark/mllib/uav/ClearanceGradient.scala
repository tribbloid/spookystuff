package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.point.NED
import org.apache.spark.mllib.linalg.BLAS

object ClearanceGradient {

  def t4MinimalDist(
                     A1: NED.C,
                     B1: NED.C,
                     A2: NED.C,
                     B2: NED.C
                   ): (Double, Double) = {

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
        clamp(0.0, 0.0)
      case (0.0, _) =>
        val t1 = 0
        val t2 = C2.t * M / CC2
        clamp(t1, t2)
      case (_, 0.0) =>
        val t2 = 0
        val t1 = - C1.t * M / CC1
        clamp(t1, t2)
      case _ =>
        val C21 = C2 * C1.t
        val G = C21 - C21.t
        val C1TGC2 = C1.t * G * C2

        def t1 = - (M.t * G * C2) / C1TGC2
        def t2 = - (M.t * G * C1) / C1TGC2

        clamp(t1, t2)
    }
  }
}

case class ClearanceGradient(
                              runner: ClearanceRunner
                            ) extends PathPlanningGradient {

  def id2Traces: Map[Int, Seq[Trace]] = runner.partitionID2Traces

  def schema = runner.schema

  val uavConf = schema.ec.spooky.getConf[UAVConf]
  val home = uavConf.home

  def findNextTraceInSamePartition(flattenIndex: Int,
                                   partitionID: Int): Option[Trace] = {
    for (i <- (flattenIndex + 1) until flatten.size) {
      val (nextPID, trace) = flatten(i)
      if (nextPID != partitionID) return None
      trace.foreach {
        case vin: VectorIndexedNav => return Some(trace)
        case _ =>
      }
    }
    None
  }

  override def compute(
                        data: MLVec,
                        label: Double, // ignored
                        weights: MLVec,
                        cumGradient: MLVec
                      ): Double = {

    val flattenIndex1_2 = data.asInstanceOf[MLSVec].indices
    assert(flattenIndex1_2.length == 2)

    val traces1_2: Array[(Trace, Option[Trace])] = flattenIndex1_2.map {
      i =>
        val (partitionID, trace) = flatten(i)
        var nextTraceOpt = findNextTraceInSamePartition(i, partitionID)
        trace -> nextTraceOpt
    }

    val nav_locations1_2 = traces1_2.map {
      tuple =>
        val nav_locations = {
          val navs = tuple._1.collect {
            case v: VectorIndexedNav => v
          }
          navs.map {
            nav =>
              nav -> nav.shiftAllByWeight(weights.toBreeze)
                .getLocation(schema)
          }
        }
        val nextNav_locationOpt = tuple._2.map {
          nextTrace =>
            val nextNav = nextTrace.find(_.isInstanceOf[VectorIndexedNav]).get.asInstanceOf[VectorIndexedNav]
            nextNav -> nextNav.shiftAllByWeight(weights.toBreeze)
              .getLocation(schema)
        }
        nav_locations ++ nextNav_locationOpt
    }
    val nav_coordinates1_2 = nav_locations1_2.map {
      nav_locations =>
        nav_locations.map {
          nav_location =>
            nav_location._1 -> nav_location._2.getCoordinate(NED, home).get
        }
    }
    val Array(nav_coordinates1, nav_coordinates2) = nav_coordinates1_2

    var cumViolation = 0.0
    for (
      i <- 0 until (nav_coordinates1.size - 1);
      j <- 0 until (nav_coordinates2.size - 1)
    ) {

      case class Notation(v: (VectorIndexedNav, NED.C)) {

        val vin = v._1
        val coordinate = v._2
        val vector = coordinate.vector

        var nabla: Vec = _
      }

      val A1 = Notation(nav_coordinates1(i))
      val B1 = Notation(nav_coordinates1(i+1))
      val A2 = Notation(nav_coordinates2(i))
      val B2 = Notation(nav_coordinates2(i+1))

      val (t1, t2) = ClearanceGradient.t4MinimalDist(
        A1.coordinate, B1.coordinate,
        A2.coordinate, B2.coordinate
      )

      val M = A1.vector - A2.vector
      val C1 = B1.vector - A1.vector
      val C2 = B2.vector - A2.vector

      val P = M + t1*C1 - t2*C2
      val DSquare = P dot P
      val D = Math.sqrt(DSquare)
      val violation = runner.outer.traffic - D

      if (violation > 0) {

        val ratio = violation/D // TODO: or square of it? I'm confused
        val aug = ratio*ratio

        A1.nabla = (1 - t1) * aug * P
        B1.nabla = t1 * aug * P
        A2.nabla = (t2 - 1) * aug * P
        B2.nabla = - t2 * aug * P

        val concat: Seq[(Int, Double)] = Seq(A1, B1, A2, B2).flatMap {
          notation =>
            val nabla: Vec = notation.nabla
            val updated1: Vec = notation.vin.nav.rewrite(nabla, schema)
            val updated2: Vec = runner.outer.locationShifter.rewrite(updated1, schema)

            notation.vin.weightIndex.zip(updated2.toArray)
        }

        val concatGradVec = new MLSVec(
          weights.size,
          concat.map(_._1).toArray,
          concat.map(_._2).toArray
        )
        BLAS.axpy(1.0, concatGradVec, cumGradient)
      }
      cumViolation += violation
    }
    println(s"========= cumViolation: $cumViolation =========")
    cumViolation
  }
}
