package com.tribbloids.spookystuff.graph

case class Mutator[T1 <: GraphSystem](
    nodeFn: T1#NodeData => T1#NodeData,
    edgeFn: T1#EdgeData => T1#EdgeData
)

object Mutator {

  def noop[T <: GraphSystem] = Mutator(identity[T#NodeData], identity[T#EdgeData])
}
