package org.apache.spark.ml.uav

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.uav.planning.TrafficControls.Avoid
import org.apache.spark.mllib.linalg.BLAS

object AvoidGradient {

  def apply(runner: AvoidSGDRunner): AvoidGradient = AvoidGradient(
    runner.pid2Traces_resampled,
    runner.outer,
    runner.schema
  )
}

case class AvoidGradient(
                              id2Traces: Map[Int, Seq[Trace]],
                              self: Avoid,
                              schema: SpookySchema
                            ) extends PathPlanningGradient {

  override def constraint = self.constraint

  //  val uavConf = schema.ec.spooky.getConf[UAVConf]
  //  val home = uavConf.home

  def findNextTraceInSamePartition(flattenIndex: Int,
                                   partitionID: Int): Option[Trace] = {

    for (i <- (flattenIndex + 1) until flatten.size) {
      val (nextPID, trace) = flatten(i)
      if (nextPID != partitionID) return None
      trace.foreach {
        case ven: VectorEncodedNav => return Some(trace)
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

    val encoded1_2: Array[List[VectorEncodedNav]] = traces1_2.map {
      tuple =>
        val shifted = {
          val encoded = tuple._1.collect {
            case v: VectorEncodedNav => v
          }
          encoded.map {
            nav =>
              nav.shiftByWeights(weights.asBreeze)
          }
        }
        val nextShiftedOpt = tuple._2.map {
          nextTrace =>
            val nextNav = nextTrace.find(_.isInstanceOf[VectorEncodedNav])
              .get.asInstanceOf[VectorEncodedNav]
            nextNav.shiftByWeights(weights.asBreeze)
        }
        shifted ++ nextShiftedOpt
    }
    //    val nav_coordinates1_2 = encoded1_2.map {
    //      nav_locations =>
    //        nav_locations.map {
    //          nav_location =>
    //            nav_location._1 -> nav_location._2.getCoordinate(NED, home).get
    //        }
    //    }
    val Array(encoded1, encoded2) = encoded1_2

    var cumViolation = 0.0
    for (
      i <- 0 until (encoded1.size - 1);
      j <- 0 until (encoded2.size - 1)
    ) {

      case class Notation(v: VectorEncodedNav) {

        var offset: Vec = _
      }

      val A1 = Notation(encoded1(i))
      val B1 = Notation(encoded1(i+1))
      val A2 = Notation(encoded2(i))
      val B2 = Notation(encoded2(i+1))

      val twoLines: TwoLines = TwoLines(A1.v, B1.v, A2.v, B2.v)
      import twoLines._

      val violation = self._traffic - D

      if (violation > 0) {

        val scaling = {
          //          val ratio = violation/D //L2 loss
          val ratio = 1/D //hinge loss
          ratio
        }

        A1.offset = (1 - t1) * scaling * P
        B1.offset = t1 * scaling * P
        A2.offset = (t2 - 1) * scaling * P
        B2.offset = - t2 * scaling * P

        val concat: Seq[(Int, Double)] = Seq(A1, B1, A2, B2).flatMap {
          notation =>
            var nabla: Vec = - notation.offset

            (notation.v.constraint.toSeq ++ this.constraint).foreach {
              cc =>
                nabla = cc.rewrite(nabla, schema)
            }

            notation.v.weightIndices.zip(nabla.toArray)
        }

        val concatGradVec = new MLSVec(
          weights.size,
          concat.map(_._1).toArray,
          concat.map(_._2).toArray
        )
        BLAS.axpy(1.0, concatGradVec, cumGradient)
        cumViolation += violation
      }
    }
    //    LoggerFactory.getLogger(this.getClass).info(
    //      s"========= cumViolation: $cumViolation ========="
    //    )
    cumViolation
  }
}
