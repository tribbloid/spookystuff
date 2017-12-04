package org.apache.spark.ml.uav

import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.planning.Constraint
import org.apache.spark.mllib.optimization.Gradient
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

trait PathPlanningGradient extends Gradient {

  def schema: SpookySchema
  def constraint: Option[Constraint]

  // id = TaskContext.get.partitionID
  def id2Traces: Map[Int, Seq[Trace]]

  val encodedBuffer = ArrayBuffer.empty[VectorEncodedNav]
  val id2Traces_withEncoded: Map[Int, Seq[Trace]] = {
    var weightDim = 0
    var dataDim = 0
    val id2VIT = id2Traces.mapValues {
      traces: Seq[Trace] =>
        traces.map {
          trace: Trace =>
            val encoded: Trace = trace.map {
              action =>
                //TODO: CAUTION! scala 2.10 compiler will trigger a bug once switching to match
                if (action.isInstanceOf[UAVNavigation]) {
                  val nav = action.asInstanceOf[UAVNavigation]
                  val wi = weightDim until (weightDim + nav.vectorDim)
                  val di = dataDim
                  weightDim += nav.vectorDim
                  dataDim += 1
                  val withSchema = nav.WSchema(schema)
                  val result: VectorEncodedNav = VectorEncodedNav(withSchema, wi, di)
                  encodedBuffer += result
                  result: Action
                }
                else {
                  action
                }
            }
            encoded
        }
    }
      .map(identity)
    id2VIT
  }

  lazy val flatten: Seq[(Int, Trace)] = {
    val seq = id2Traces_withEncoded.toSeq
    val result = seq
      .flatMap(
        tuple =>
          tuple._2.map(tuple._1 -> _)
      )
    result
  }
  lazy val numTraces = flatten.size

  /**
    * yield spark vector RDD
    * non-zero index representing the participating index of traces
    * index of 1 represents the first
    * index of 1 also represents the second
    * @return RDD[Label (always 0) -> SparseVector (index of operand trace in expanded)]
    */
  def generateDataRDD: RDD[(Double, MLVec)] = {

    // this operation should be distributed
    val sc = schema.spooky.sparkContext

    val numTraces = this.numTraces
    val pairRDD: RDD[(Int, Int)] = sc.parallelize(0 until numTraces)
      .flatMap {
        i =>
          val js = 0 until numTraces
          js.map(j => i -> j)
      }

    val expandedIDs = flatten.map(_._1).toList
    val dataRDD = pairRDD.flatMap {
      case (i, j) =>
        val id_i = expandedIDs(i)
        val id_j = expandedIDs(j)
        if (id_i < id_j)
          Some(new MLSVec(numTraces, Array(i, j), Array(1, 1)))
        else
          None
      case _ =>
        None
    }
    val withLabelRDD: RDD[(Double, MLVec)] = dataRDD.map {0.0 -> _}
    withLabelRDD
  }

  val initializationNoise = 0.01
  def initialWeights: MLVec = {
    val array = encodedBuffer.flatMap {
      ven =>
        var vec = Vec.fill(ven.vectorDim){
          Random.nextDouble()* initializationNoise
        }
        (ven.self.outer.constraint.toSeq ++ this.constraint).foreach {
          cc =>
            vec = cc.rewrite(vec, schema)
        }
        vec.toArray
    }
      .toArray

    new MLDVec(array)
  }
}
