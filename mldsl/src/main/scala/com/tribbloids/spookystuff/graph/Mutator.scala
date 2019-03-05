package com.tribbloids.spookystuff.graph

case class Mutator[T <: GraphSystem](
    nodeFn: Option[T#NodeData] => Option[T#NodeData],
    edgeFn: Option[T#EdgeData] => Option[T#EdgeData]
) {}

object Mutator {

  def noop[T <: GraphSystem] = Mutator[T](identity, identity)
}
