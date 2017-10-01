package org.apache.spark.mllib.optimization

import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.caching.ConcurrentCache
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.actions.UAVNavigation

trait PlanningGradient extends Gradient {

  def traces: Array[Trace]

  val (
    weightDim: Int,
    dataDim: Int,
    n_indices: Array[List[Action]]
    ) = {
    var weightDim = 0
    var dataDim = 0
    val a_wIndex: Array[List[Action]] = traces.map {
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
    (weightDim, dataDim, a_wIndex)
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

    lazy val n_indicesDelta: Array[List[Action]] = n_indices.map {
      list =>
        list.map {
          case n_index: Nav_Index =>
            val delta = n_index.computeDelta(weights)
            n_index.copy(
              nav = delta
            )
          case others =>
            others
        }
    }
  }
}

case class Nav_Index(
                      nav: UAVNavigation,
                      weightIndex: Range,
                      dataIndex: Int //TODO: currently useless, remove?
                    ) extends UAVNavigation{

  def computeDelta(weights: Vec): UAVNavigation = {
    val range = weightIndex
    val sliced = weights.toArray.slice(range.start, range.end)
    nav.computeDelta(sliced)
    nav
  }

  override def doExe(session: Session) = nav.doExe(session)
  override def getLocation(trace: Trace, schema: DataRowSchema) = nav.getLocation(trace, schema)
}
