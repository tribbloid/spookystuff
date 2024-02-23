package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Element.Edge
import com.tribbloids.spookystuff.commons.Types
import com.tribbloids.spookystuff.commons.collection.MultiMapOps

import scala.collection.mutable
import scala.reflect.ClassTag

trait StaticGraph[T <: Domain] extends Module[T] {

  def evict_!(edge: _Edge): Unit
  def connect_!(edge: _Edge): Unit

  def getLinkedNodes(ids: Seq[ID]): Map[ID, _NodeTriplet]
  def getEdges(ids: Seq[(ID, ID)]): MultiMapOps.Immutable[(ID, ID), _Edge]
}

object StaticGraph {

  trait Builder[D <: Domain] extends Algebra.Aliases[D] {

    type GG <: StaticGraph[D]
    protected def getCtg(
        implicit
        ev: ClassTag[GG]
    ): ClassTag[GG] = ev
    implicit def ctg: ClassTag[GG]
    final lazy val _ctg: ClassTag[GG] = ctg

//    implicit val algebra: Algebra[D]

    def fromSeq(
        nodes: Seq[_NodeLike],
        edges: Seq[_Edge],
        node_+ : Types.Compose[NodeData] = nodeAlgebra.add
    ): GG

    final def fromModule(graph: _Module): GG = {

      graph match {
        case v: _NodeLike => this.fromSeq(Seq(v), Nil)
        case v: _Edge     => this.fromSeq(Nil, Seq(v))
        case _ctg(v)      => v
      }
    }
    def union(v1: GG, v2: GG, node_+ : Types.Compose[NodeData] = nodeAlgebra.add): GG

    // TODO: this API need to change to facilitate big Heads and Tails in the format of RDD
    /**
      * heads of base & tails of top are superseded by their merged result
      * @param node_+
      *   binary operation to combine data from 2 nodes
      * @param edge_+
      *   binary operation to combine data from 2 edges
      * @return
      *   merged graph -> mappings that converts evicted edges to created edges
      */
    def serial(
        base: (GG, _Heads),
        top: (GG, _Tails),
        node_+ : Types.Compose[NodeData] = nodeAlgebra.add,
        edge_+ : Types.Compose[EdgeData] = edgeAlgebra.add
    ): (GG, Map[_Edge, _Edge]) = {

      val uu: GG = union(base._1, top._1, node_+)
      val toBeRemoved: Seq[Edge[D]] = base._2.seq ++ top._2.seq
      toBeRemoved.foreach { v: Edge[D] =>
        uu.evict_!(v)
      }

      val edgeConversion = mutable.Map[_Edge, _Edge]()

      for (
        src <- base._2.seq;
        tgt <- top._2.seq
      ) {

        val reduced = Edge[D](
          edge_+(src.data, tgt.data),
          (src.qualifier ++ tgt.qualifier).distinct,
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
