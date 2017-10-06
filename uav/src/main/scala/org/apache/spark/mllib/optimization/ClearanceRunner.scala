package org.apache.spark.mllib.optimization

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.planning.traffic.Clearance
import com.tribbloids.spookystuff.utils.NOTSerializable

case class ClearanceRunner(
                            traces: Array[Trace],
                            schema: DataRowSchema,
                            outer: Clearance
                          ) extends NOTSerializable {

  val gradient = ClearanceGradient(this)
  val updater = new SquaredL2Updater()

  val dim = gradient.dataDim

  def solve: Array[Trace] = {
    val data = gradient.dataRDD
    val (weights, convergence) = GradientDescent.runMiniBatchSGD(
      data,
      gradient,
      updater,
      1.0,
      100,
      1.0,
      1.0,
      gradient.initializeWeight
    )
    val solution = gradient.withWeights(weights)
      .shiftLocation
    solution
  }
}
