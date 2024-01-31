package com.tribbloids.spookystuff.graph

trait EdgeFilter[T <: Domain] {

  def apply(graph: StaticGraph[T]): Seq[Element.Edge[T]]
}

object EdgeFilter {

  // implicit conversion to be added into companion object of implementation of domain
}
