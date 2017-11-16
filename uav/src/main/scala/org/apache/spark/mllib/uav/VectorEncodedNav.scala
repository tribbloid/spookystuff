package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.actions.ActionPlaceholder
import com.tribbloids.spookystuff.uav.actions.UAVNavigation

import scala.language.implicitConversions

object VectorEncodedNav {

  implicit def toNavWSchema(v: VectorEncodedNav) = v.self
  implicit def toNav(v: VectorEncodedNav) = v.self.outer
}

case class VectorEncodedNav(
                             self: UAVNavigation#WSchema,
                             weightIndices: Range,
                             seqIndex: Int = -1 //TODO: currently useless, remove?
                           ) extends ActionPlaceholder {

  // TODO: expensive! this should have mnemonics
  def shiftByWeights(weights: Vec): VectorEncodedNav = {
    val range = weightIndices
    val sliced = weights(range)
    val neo = self.outer.shift(sliced)
    this.copy(self = neo.WSchema(this.self.schema))
  }
}
