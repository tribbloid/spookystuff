package org.apache.spark.mllib.optimization

import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.planning.traffic.{Clearance, CollisionAvoidance}
import com.tribbloids.spookystuff.uav.spatial.NED
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.BLAS
import org.apache.spark.rdd.RDD

object ClearanceGradient {


  def t4MinimalDist(
                     A1: NED.V,
                     B1: NED.V,
                     A2: NED.V,
                     B2: NED.V
                   ): (Double, Double) = {

    val M = A1.vector - A2.vector
    val C1 = B1.vector - A1.vector
    val C2 = B2.vector - A2.vector

    val C21 = C2 * C1.t
    val G = C21 - C21.t

    val C1TGC2 = C1.t * G * C2

    val _t1 = - (M.t * G * C2) / C1TGC2
    val _t2 = - (M.t * G * C1) / C1TGC2

    val t1 = Math.max(Math.min(1.0, _t1), 0.0)
    val t2 = Math.max(Math.min(1.0, _t2), 0.0)

    t1 -> t2
  }
}

case class ClearanceGradient(
                              traces: Array[Trace],
                              schema: DataRowSchema,
                              outer: Clearance
                            ) extends PlanningGradient {

  val uavConf = schema.ec.spooky.getConf[UAVConf]
  val home = uavConf.home

  override def compute(
                        data: MLVec,
                        label: Double, // ignored
                        weights: MLVec,
                        cumGradient: MLVec
                      ): Double = {

    assert(data.size == 2)

    val withWeights = this.withWeights(weights)

    val n_indexDelta: Array[List[Action]] = withWeights.n_indicesDelta

    val first = data(0).toInt
    val second = data(1).toInt

    val firstNavs = n_indexDelta(first).collect {
      case v: Nav_Index => v
    }
    val secondNavs = n_indexDelta(second).collect {
      case v: Nav_Index => v
    }

    var cumViolation = 0.0
    for (
      i <- 0 until (firstNavs.size - 1);
      j <- 0 until (secondNavs.size - 1)
    ) {
      val chosen1 = Seq(
        firstNavs(i),
        firstNavs(i + 1)
      )
      val chosen2 = Seq(
        secondNavs(i),
        secondNavs(i + 1)
      )
      val Seq(a1, b1) = chosen1.map {
        v =>
          v.getLocation(n_indexDelta(first), schema)
            .getCoordinate(NED, home).get
      }
      val Seq(a2, b2) = chosen2.map {
        v =>
          v.getLocation(n_indexDelta(second), schema)
            .getCoordinate(NED, home).get
      }

      val (t1, t2) = ClearanceGradient.t4MinimalDist(
        a1, b1,
        a2, b2
      )

      val m = a1.vector - a2.vector
      val c1 = b1.vector - a1.vector
      val c2 = b2.vector - a2.vector

      val p = m + t1*c1 - t2*c2
      val dSquare = p dot p

      val violation = Math.pow(outer.traffic, 2.0) - dSquare

      if (violation > 0) {
        val nablas1 = Seq((1 - t1)*p, t1*p)
        val nablas2 = Seq((1 - t2)*p, t2*p)

        val concat: Seq[(Int, Double)] = (chosen1 ++ chosen2).zip(nablas1 ++ nablas2)
          .flatMap {
            tuple =>
              val vec: Vec = tuple._2
              val updated1: Vec = tuple._1.nav.rewrite(vec, schema)
              val updated2: Vec = outer.locationUpdater.rewrite(updated1, schema)

              tuple._1.weightIndex.zip(updated2.toArray)
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
    cumViolation
  }

  def generateDataRDD(sc: SparkContext): RDD[MLVec] = {

    val pair = for (
      i <- 0 until dataDim;
      j <- 0 until dataDim
    ) yield {
      i -> j
    }
    val result = pair.flatMap {
      case (i, j) if i < j =>
        Some(new MLDVec(Array(i, j)))
      case _ =>
        None
    }
    sc.parallelize(result)
  }
}