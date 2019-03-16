package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Element.Edge
import com.tribbloids.spookystuff.utils.{CommonTypes, MultiMapView}

import scala.collection.mutable
import scala.language.higherKinds

trait StaticGraph[T <: Domain] extends Module[T] {

  def evict_!(edge: _Edge): Unit
  def connect_!(edge: _Edge): Unit

  def getLinkedNodes(ids: Seq[ID]): Map[ID, _LinkedNode]
  def getEdges(ids: Seq[(ID, ID)]): MultiMapView.Immutable[(ID, ID), _Edge]
}

object StaticGraph {

  trait Builder[D <: Domain, GProto[T <: Domain] <: StaticGraph[T]] extends Algebra.Sugars[D] {

    type GG = GProto[D]

    implicit val algebra: Algebra[D]

    def fromSeq(
        nodes: Seq[_NodeLike],
        edges: Seq[_Edge]
    ): GG

    def fromModule(v: _Module): GG

    def union(v1: GG, v2: GG, node_+ : CommonTypes.Binary[NodeData] = nodeAlgebra.plus): GG

    //TODO: this API need to change to facilitate big Heads and Tails in the format of RDD
    /**
      * @param node_+ binary operation to combine data from 2 nodes
      * @param edge_+ binary operation to combine data from 2 edges
      * @return merged graph -> mappings that converts evicted edges to created edges
      */
    def merge(
        base: (GG, _Heads),
        top: (GG, _Tails),
        node_+ : CommonTypes.Binary[NodeData] = nodeAlgebra.plus,
        edge_+ : CommonTypes.Binary[EdgeData] = edgeAlgebra.plus
    ): (GG, Map[_Edge, _Edge]) = {

      val uu: GG = union(base._1, top._1, node_+)
      val toBeRemoved: Seq[Edge[D]] = base._2.seq ++ top._2.seq
      toBeRemoved.foreach { v: Edge[D] =>
        uu.evict_!(v)
      }

      val edgeConversion = mutable.Map[_Edge, _Edge]()

      for (src <- base._2.seq;
           tgt <- top._2.seq) {

        val reduced = Edge[D](
          edge_+(src.data, tgt.data),
          src.from -> tgt.to
        )

        edgeConversion.put(src, reduced)
        edgeConversion.put(tgt, reduced)

        uu.connect_!(reduced)
      }
      uu -> edgeConversion.toMap
    }
  }
}
