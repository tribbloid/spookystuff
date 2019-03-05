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

    def fromSeq(
        nodes: Seq[_NodeLike],
        edges: Seq[_Edge]
    ): G

    def fromComponent(v: _GraphComponent): G

    def union(v1: G, v2: G, nodeReducer: CommonTypes.Binary[Option[NodeData]]): G

    //TODO: this API need to change to facilitate big Heads and Tails in the format of RDD
    def merge(
        base: (G, _Heads),
        top: (G, _Tails),
        nodeReducer: CommonTypes.Binary[Option[NodeData]] = nodeAlgebra.combineMonads,
        edgeReducer: CommonTypes.Binary[Option[EdgeData]] = edgeAlgebra.combineMonads
    ): G = {

      val uu: G = union(base._1, top._1, nodeReducer)
      val toBeRemoved: Seq[Edge[T]] = base._2.seq ++ top._2.seq
      toBeRemoved.foreach { v: Edge[T] =>
        uu.evict_!(v)
      }

      for (src <- base._2.seq;
           tgt <- top._2.seq) {

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
