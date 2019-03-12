package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Element.Edge
import com.tribbloids.spookystuff.utils.{CommonTypes, MultiMapView}

import scala.language.higherKinds

trait StaticGraph[+T <: Domain] extends Module[T] {

  def evict_!(edge: _Edge): Unit
  def connect_!(edge: _Edge): Unit

  def getLinkedNodes(ids: Seq[ID]): Map[ID, _LinkedNode]
  def getEdges(ids: Seq[(ID, ID)]): MultiMapView.Immutable[(ID, ID), _Edge]
}

object StaticGraph {

  trait Builder[T <: Domain, G <: StaticGraph[T]] extends Algebra.Sugars[T] {

    implicit val algebra: Algebra[T]

    def fromSeq(
        nodes: Seq[_NodeLike],
        edges: Seq[_Edge]
    ): G

    def fromModule(v: _Module): G

    def union(v1: G, v2: G, nodeReducer: CommonTypes.Binary[NodeData] = nodeAlgebra.combine): G

    //TODO: this API need to change to facilitate big Heads and Tails in the format of RDD
    def merge(
        base: (G, _Heads),
        top: (G, _Tails),
        nodeReducer: CommonTypes.Binary[NodeData] = nodeAlgebra.combine,
        edgeReducer: CommonTypes.Binary[EdgeData] = edgeAlgebra.combine
    ): G = {

      val uu: G = union(base._1, top._1, nodeReducer)
      val toBeRemoved: Seq[Edge[T]] = base._2.seq ++ top._2.seq
      toBeRemoved.foreach { v: Edge[T] =>
        uu.evict_!(v)
      }

      for (src <- base._2.seq;
           tgt <- top._2.seq) {

        val reduced = Edge[T](
          edgeReducer(src.data, tgt.data),
          src.from -> tgt.to
        )
        uu.connect_!(reduced)
      }
      uu
    }
  }
}
