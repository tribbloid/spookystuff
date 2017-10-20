package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.actions.{ActionPlaceholder, Trace}
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import org.apache.spark.mllib.optimization.Gradient
import org.apache.spark.rdd.RDD

import scala.util.Random

trait PathPlanningGradient extends Gradient {

  def schema: DataRowSchema

  // id = TaskContext.get.partitionID
  def id2Traces: Map[Int, Seq[Trace]]

  lazy val numPartitions = id2Traces.size

  val (
    d_weight: Int,
    d_data: Int,
    id2Traces_indexed: Map[Int, Seq[Trace]]
    ) = {
    var weightDim = 0
    var dataDim = 0
    val id2Traces_indexed = id2Traces.mapValues {
      traces =>
        traces.map {
          trace =>
            val indexed = trace.map {
              case v: UAVNavigation =>
                val wi = weightDim until (weightDim + v.vectorDim)
                val di = dataDim
                weightDim += v.vectorDim
                VectorIndexedNav(v, wi, di)
              case v =>
                v
            }
            indexed
        }
    }
      .map(identity)
    (weightDim, dataDim, id2Traces_indexed)
  }

  lazy val flatten: Seq[(Int, Trace)] = id2Traces_indexed.flatMap(
    tuple =>
      tuple._2.map(v => tuple._1 -> v)
  )
    .toSeq
  lazy val numTraces = flatten.size

  //  val cache = ConcurrentCache[Vec, WithWeights]()
  //  def withWeights(weights: MLVec): WithWeights = {
  //
  //    val _weights: Vec = weights.toBreeze
  //    cache.getOrElseUpdate(
  //      _weights,
  //      WithWeights(_weights)
  //    )
  //  }

  //  sealed case class WithWeights(weights: Vec) {
  //    assert(weights.size == PathPlanningGradient.this.d_weight)
  //
  //    lazy val shiftLocation: Array[List[Action]] = nav_indices.map {
  //      list =>
  //        list.map {
  //          case n_index: IndexedNav =>
  //            val delta = n_index.shiftLocation(weights)
  //            delta
  //          case others =>
  //            others
  //        }
  //    }
  //  }

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
    val withLabelRDD: RDD[(Double, MLVec)] = dataRDD.map {
      v =>
        0.0 -> v
    }
    withLabelRDD
  }

  lazy val initializationNoise = 0.01
  def initializeWeight: MLVec = {
    val array = Array.fill(d_weight){
      Random.nextDouble()* initializationNoise
    }

    new MLDVec(array)
  }
}

case class VectorIndexedNav(
                             nav: UAVNavigation,
                             weightIndex: Range,
                             seqID: Int //TODO: currently useless, remove?
                           ) extends ActionPlaceholder {

  // TODO: expensive! this should have mnemonics
  def shiftAllByWeight(weights: Vec): UAVNavigation = {
    val range = weightIndex
    val sliced = weights(range)
    nav.shift(sliced)
    nav
  }
}

case class VectorIndexedTrace(
                               trace: Trace
                             ) {

  def shiftLocationByWeight(weights: Vec): Trace = {
    trace.map {
      case vin: VectorIndexedNav =>
        vin.shiftAllByWeight(weights)
      case v@ _ =>
        v
    }
  }
}
