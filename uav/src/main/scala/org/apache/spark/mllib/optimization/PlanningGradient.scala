package org.apache.spark.mllib.optimization

import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.caching.ConcurrentCache
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import org.apache.spark.rdd.RDD

import scala.util.Random

trait PlanningGradient extends Gradient {

  def traces: Array[Trace]
  def schema: DataRowSchema
  lazy val initializationNoise = 0.01

  val (
    weightDim: Int,
    dataDim: Int,
    nav_indices: Array[List[Action]]
    ) = {
    var weightDim = 0
    var dataDim = 0
    val nav_indices: Array[List[Action]] = traces.map {
      trace =>
        val features = trace.map {
          case v: UAVNavigation =>
            val wi = weightDim until (weightDim + v.vectorDim)
            val di = dataDim
            weightDim += v.vectorDim
            Nav_Index(v, wi, di)
          case others =>
            others
        }
        features
    }
    (weightDim, dataDim, nav_indices)
  }

  val cache = ConcurrentCache[Vec, WithWeights]()
  def withWeights(weights: MLVec): WithWeights = {

    val _weights: Vec = weights.toBreeze
    cache.getOrElseUpdate(
      _weights,
      WithWeights(_weights)
    )
  }

  sealed case class WithWeights(weights: Vec) {
    assert(weights.size == PlanningGradient.this.weightDim)

    lazy val shiftLocation: Array[List[Action]] = nav_indices.map {
      list =>
        list.map {
          case n_index: Nav_Index =>
            val delta = n_index.shiftLocation(weights)
            delta
          case others =>
            others
        }
    }
  }

  /**
    * yield spark vector RDD
    * non-zero index representing the participating index of traces
    * index of 1 represents the first
    * index of 1 also represents the second
    * @return
    */
  lazy val dataRDD: RDD[(Double, MLVec)] = {

    val dim = nav_indices.length
    val pair = for (
      i <- 0 until dim;
      j <- 0 until dim
    ) yield {
      i -> j
    }
    val result = pair.flatMap {
      case (i, j) if i < j =>
        Some(new MLSVec(dim, Array(i, j), Array(1, 1)))
      case _ =>
        None
    }
    val withLabel = result.map {
      v =>
        0.0 -> v
    }
    val sc = schema.spooky.sparkContext
    sc.parallelize(withLabel)
  }

  def initializeWeight: MLVec = {
    val array = Array.fill(weightDim){
      Random.nextDouble()* initializationNoise
    }

    new MLDVec(array)
  }
}

case class Nav_Index(
                      nav: UAVNavigation,
                      weightIndex: Range,
                      dataIndex: Int //TODO: currently useless, remove?
                    ) extends UAVNavigation{

  def shiftLocation(weights: Vec): UAVNavigation = {
    val range = weightIndex
    val sliced = weights.toArray.slice(range.start, range.end)
    nav.shiftLocation(sliced)
    nav
  }

  override def doExe(session: Session) = nav.doExe(session)
  override def getLocation(trace: Trace, schema: DataRowSchema) = nav.getLocation(trace, schema)
}
