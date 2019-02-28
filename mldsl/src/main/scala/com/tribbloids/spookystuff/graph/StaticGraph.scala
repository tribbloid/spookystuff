package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Element.Edge
import com.tribbloids.spookystuff.utils.CommonTypes

import scala.language.higherKinds

trait StaticGraph[T <: GraphSystem] extends GraphComponent[T] {

  def evict_!(edge: _Edge): Unit
  def connect_!(edge: _Edge): Unit
}

object StaticGraph {

  abstract class Builder[T <: GraphSystem, G <: StaticGraph[T]](
      implicit val systemBuilder: GraphSystem.Builder[T]
  ) extends GraphSystem.Sugars[T] {

    def convert(v: _GraphComponent): G

    def _union(v1: G, v2: G, nodeReducer: CommonTypes.Binary[NodeData]): G

    def union(
        v1: _GraphComponent,
        v2: _GraphComponent,
        nodeReducer: CommonTypes.Binary[NodeData] = nodeAlgebra.combine
    ): G = {

      _union(convert(v1), convert(v2), nodeReducer)
    }

    //TODO: this API need to change to facilitate big Heads and Tails in the format of RDD
    def merge(
        base: (_GraphComponent, _Heads),
        top: (_GraphComponent, _Tails),
        nodeReducer: CommonTypes.Binary[NodeData] = nodeAlgebra.combine,
        edgeReducer: CommonTypes.Binary[EdgeData] = edgeAlgebra.combine
    ): G = {

      val uu: G = union(base._1, top._1, nodeReducer)
      val toBeRemoved: Seq[Edge[T]] = base._2.vs ++ top._2.vs
      toBeRemoved.foreach { v: Edge[T] =>
        uu.evict_!(v)
      }

      for (src <- base._2.vs;
           tgt <- top._2.vs) {

        val reduced = Edge[T](
          edgeReducer(src.info, tgt.info),
          src.from -> tgt.to
        )
        uu.connect_!(reduced)
      }
      uu
    }
  }

  object Builder {}

}
