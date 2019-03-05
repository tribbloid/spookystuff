package com.tribbloids.spookystuff.graph

import scala.language.higherKinds

trait DSL[T <: GraphSystem, G <: StaticGraph[T]] extends GraphSystem.Sugars[T] {

  def impl: StaticGraph.Builder[T, G]

  override implicit val systemBuilder: GraphSystem.Builder[T] = impl.systemBuilder

  trait Facet
}

object DSL {

//  trait Builder {
//
//    def fromEdge[T <: System](
//        v: Edge[T]
//    ): DSL[T]
//  }

//  case class ParserFacet[T <: System](
//      graph: System#StaticGraph
//  ) extends DSL[T] {}

}
