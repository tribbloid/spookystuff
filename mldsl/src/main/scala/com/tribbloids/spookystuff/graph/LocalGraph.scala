package com.tribbloids.spookystuff.graph
import com.tribbloids.spookystuff.graph.Element.{Edge, LinkedNode}
import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator
import com.tribbloids.spookystuff.utils.{CommonTypes, OrderedMultiMapView}

import scala.collection.mutable

//optimised for speed rather than memory usage
//NOT thread safe!
//
//TODO: this may leverage an existing java/scala graph library
//TODO: also need SparkGraphImpl, use GraphX or GraphFrame depending on their maturity
case class LocalGraph[T <: GraphSystem] private (
    nodeMap: mutable.Map[T#ID, LinkedNode[T]],
    edgeMap: OrderedMultiMapView.Mutable[(T#ID, T#ID), Edge[T]]
)(
    override implicit val systemBuilder: GraphSystem.Builder[T]
) extends StaticGraph[T] {

  override def _replicate(m: _Mutator)(implicit idRotator: Rotator[ID] = idAlgebra.createRotator()) = {
    LocalGraph
      .Builder[T]()
      .fromSeq(
        nodeMap.values.toSeq.map(_.replicate(m)),
        edgeMap.values.toSeq.flatMap(_.map(_.replicate(m)))
      )
  }

  def evict_!(edge: _Edge): Unit = {

    val filtered = edgeMap
      .filterValue(edge.ids) { v =>
        v != edge
      }
      .getOrElse(Nil)

    if (filtered.isEmpty) {
      nodeMap(edge.from).outbound.remove(edge.to)
      nodeMap(edge.to).inbound.remove(edge.from)
    }
  }

  // adding nodeID that doesn't exist is strongly prohibited
  def connect_!(edge: _Edge): Unit = {

    val fromNode = nodeMap(edge.from)
    fromNode.outbound += edge.to

    val toNode = nodeMap(edge.to)
    toNode.inbound += edge.from

    edgeMap.put1(edge.ids, edge)
  }
}

object LocalGraph {

  case class Builder[T <: GraphSystem](
      implicit override val systemBuilder: GraphSystem.Builder[T]
  ) extends StaticGraph.Builder[T, LocalGraph[T]] {

    type _LocalGraph = LocalGraph[T]

    override def fromSeq(
        nodes: Seq[_NodeLike],
        edges: Seq[_Edge]
    ): _LocalGraph = {

      val existingIDs = nodes.map(_._id).toSet

      val missingIDs: Seq[T#ID] = edges
        .flatMap { v =>
          Seq(v.ids._1, v.ids._2)
        }
        .distinct
        .filter(v => existingIDs.contains(v))

      val _nodes = nodes ++ missingIDs.map { id =>
        systemBuilder.NodeLike(_id = id)
      }

      val linkedNodes: Seq[_LinkedNode] = _nodes.map {
        case nn: _LinkedNode =>
          nn
        case nn: _NodeLike =>
          val inbound = mutable.LinkedHashSet(edges.filter(_.to == nn._id).map(_.from): _*)
          val outbound = mutable.LinkedHashSet(edges.filter(_.from == nn._id).map(_.from): _*)
          new _LinkedNode(nn, inbound, outbound): _LinkedNode
      }

      val edgeMap = OrderedMultiMapView.Mutable.empty[(ID, ID), _Edge]
      for (ee <- edges) {
        edgeMap.put1(ee.ids, ee)
      }

      new _LocalGraph(
        mutable.Map(linkedNodes.map(v => v._id -> v): _*),
        edgeMap
      )
    }

    override def fromComponent(graph: _GraphComponent): _LocalGraph = graph match {
      case v: _NodeLike   => this.fromSeq(Seq(v), Nil)
      case v: _Edge       => this.fromSeq(Nil, Seq(v))
      case v: _LocalGraph => v
    }

    protected def linkedNodeReducer(
        v1: _LinkedNode,
        v2: _LinkedNode,
        nodeReducer: CommonTypes.Binary[Option[NodeData]]
    ): _LinkedNode = {

      require(v1._id == v2._id, s"ID mismatch, ${v1._id} ~= ${v2._id}")

      val node = systemBuilder.NodeLike(nodeReducer(v1.info, v2.info), v1._id)

      val inbound = v1.inbound ++ v2.inbound
      val outbound = v1.outbound ++ v2.outbound
      new _LinkedNode(node, inbound, outbound)
    }

    def union(
        v1: _LocalGraph,
        v2: _LocalGraph,
        nodeReducer: CommonTypes.Binary[Option[NodeData]]
    ): _LocalGraph = {

      val v2Reduced: mutable.Map[T#ID, _LinkedNode] = v2.nodeMap.map {
        case (k, vv2) =>
          val reducedV = v1.nodeMap
            .get(k)
            .map { vv1 =>
              linkedNodeReducer(vv1, vv2, nodeReducer)
            }
            .getOrElse(vv2)
          k -> reducedV
      }

      val uNodeMap: mutable.Map[ID, _LinkedNode] = v1.nodeMap ++ v2Reduced
      val uEdgeMap: OrderedMultiMapView.Mutable[(ID, ID), _Edge] = v1.edgeMap +:+ v2.edgeMap

      new _LocalGraph(
        uNodeMap,
        uEdgeMap
      )
    }
  }
}
