package com.tribbloids.spookystuff.graph

case class Mutator[T <: Domain](
    nodeFn: T#NodeData => T#NodeData,
    edgeFn: T#EdgeData => T#EdgeData
) {}

object Mutator {

  def replicate[T <: Domain] = Mutator[T](identity, identity)
}
