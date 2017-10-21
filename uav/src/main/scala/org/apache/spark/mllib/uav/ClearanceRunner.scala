package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.planning.CollisionAvoidances.Clearance
import org.apache.spark.mllib.optimization.{GradientDescent, SquaredL2Updater}

/**
  *
  * @param partitionID2Traces each element represents a trace,
  *                           multiple traces can be in the same partition
  * @param schema
  * @param outer
  */
case class ClearanceRunner(
                            partitionID2Traces: Map[Int, Seq[Trace]],
                            schema: DataRowSchema,
                            outer: Clearance
                          ) {

  val gradient = ClearanceGradient(this)
  val updater = new SquaredL2Updater()

  //TODO: result may be a very large object that requires shipping
  //should optimize after PoC
  def solve: Map[Int, Seq[Trace]] = {
    val data = gradient.generateDataRDD
    val (weights, convergence) = GradientDescent.runMiniBatchSGD(
        data = data,
        gradient = gradient,
        updater = updater,
        stepSize = 1.0,
        numIterations = 100,
        regParam = 0.1,
        miniBatchFraction = 1.0,
        initialWeights = gradient.initializeWeight
      )
    val solution = gradient.id2VectorIndexedTrace.mapValues {
      array =>
        array.map {
          trace =>
            trace.map {
              action =>
                val shifted = action match {
                  case v: VectorIndexedNav =>
                    v.shiftAllByWeight(weights.toBreeze)
                  case _ =>
                    action
                }
                shifted
            }
        }
    }
    solution
  }
}
