package com.tribbloids.spookystuff.graph
import com.tribbloids.spookystuff.graph.Element.{Edge, LinkedNode}
import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator
import com.tribbloids.spookystuff.utils.{CommonTypes, MultiMapView}

import scala.collection.mutable

//optimised for speed rather than memory usage
//NOT thread safe!
//
//TODO: this may leverage an existing java/scala graph library
//TODO: also need SparkGraphImpl, use GraphX or GraphFrame depending on their maturity
case class LocalGraph[D <: Domain] private (
    nodeMap: mutable.Map[D#ID, LinkedNode[D]],
    edgeMap: MultiMapView.Mutable[(D#ID, D#ID), Edge[D]]
)(
    override implicit val algebra: Algebra[D]
) extends StaticGraph[D] {

//  {
//    insertDangling()
//  }
//
//  def insertDangling(): Unit = {
//
//    val id = algebra.DANGLING._id
//    if (!nodeMap.contains(id))
//      nodeMap.put(id, new _LinkedNode(algebra.DANGLING))
//  }

  override def _replicate(m: _Mutator)(implicit idRotator: Rotator[ID]) = {

    LocalGraph
      .BuilderImpl[D]()
      .fromSeq(
        nodeMap.values.toSeq.map(_.replicate(m)),
        edgeMap.values.toSeq.flatMap(_.map(_.replicate(m)))
      )
  }

  def removeIfExists[T](set: mutable.Set[T], v: T): Unit = {

    if (set.contains(v))
      set.remove(v)
  }

  def evict_!(edge: _Edge): Unit = {

    val filtered = edgeMap
      .filterValue(edge.from_to) { v =>
        v != edge
      }
      .getOrElse(Nil)

    if (filtered.isEmpty) {
      nodeMap.get(edge.from).foreach { node =>
        removeIfExists(node.outbound, edge.to)
      }

      nodeMap.get(edge.to).foreach { node =>
        removeIfExists(node.inbound, edge.from)
      }
    }
  }

  // adding nodeID that doesn't exist is strongly prohibited
  def connect_!(edge: _Edge): Unit = {

    val fromNode = nodeMap(edge.from)
    fromNode.outbound += edge.to

    val toNode = nodeMap(edge.to)
    toNode.inbound += edge.from

    edgeMap.put1(edge.from_to, edge)
  }

  override def getLinkedNodes(ids: Seq[ID]): Map[ID, _LinkedNode] =
    Map(ids.flatMap { id =>
      nodeMap.get(id).map(id -> _)
    }: _*)

  override def getEdges(ids: Seq[(ID, ID)]): MultiMapView.Immutable[(ID, ID), _Edge] =
    Map(ids.flatMap { pair =>
      edgeMap.get(pair).map(pair -> _)
    }: _*)
}

object LocalGraph {

  case class BuilderImpl[D <: Domain](
      implicit override val algebra: Algebra[D]
  ) extends StaticGraph.Builder[D] {

    type GG = LocalGraph[D]

    override def fromSeq(
        nodes: Seq[_NodeLike],
        edges: Seq[_Edge]
    ): GG = {

      val existingIDs = nodes.map(_._id).toSet

      val missingIDs: Seq[D#ID] = edges
        .flatMap { v =>
          Seq(v.from_to._1, v.from_to._2)
        }
        .distinct
        .filterNot(v => existingIDs.contains(v))

      val _nodes = nodes ++ missingIDs.map { id =>
        algebra.createNode(id = Some(id))
      }

      val linkedNodes: Seq[_LinkedNode] = _nodes.map {
        case nn: _LinkedNode =>
          nn
        case nn: _Node =>
          val inbound = mutable.LinkedHashSet(edges.filter(_.to == nn._id).map(_.from): _*)
          val outbound = mutable.LinkedHashSet(edges.filter(_.from == nn._id).map(_.to): _*)
          val result = new _LinkedNode(nn, inbound, outbound)
          result
      }

      val edgeMap = MultiMapView.Mutable.empty[(ID, ID), _Edge]
      for (ee <- edges) {
        edgeMap.put1(ee.from_to, ee)
      }

      new GG(
        mutable.Map(linkedNodes.map(v => v._id -> v): _*),
        edgeMap
      )(algebra)
    }

    override def fromModule(graph: _Module): GG = graph match {
      case v: _NodeLike => this.fromSeq(Seq(v), Nil)
      case v: _Edge     => this.fromSeq(Nil, Seq(v))
      case v: GG        => v
    }

    protected def linkedNodeReducer(
        v1: _LinkedNode,
        v2: _LinkedNode,
        nodeReducer: CommonTypes.Binary[NodeData]
    ): _LinkedNode = {

      require(v1._id == v2._id, s"ID mismatch, ${v1._id} ~= ${v2._id}")

      val node = algebra.createNode(nodeReducer(v1.data, v2.data), Some(v1._id))

      val inbound = v1.inbound ++ v2.inbound
      val outbound = v1.outbound ++ v2.outbound
      new _LinkedNode(node, inbound, outbound)
    }

    def union(
        v1: GG,
        v2: GG,
        nodeReducer: CommonTypes.Binary[NodeData]
    ): GG = {

      val v2Reduced: mutable.Map[D#ID, _LinkedNode] = v2.nodeMap.map {
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
      val uEdgeMap: MultiMapView.Mutable[(ID, ID), _Edge] = {
        val result = v1.edgeMap +:+ v2.edgeMap
        result.distinctAllValues()
        result
      }

      new GG(
        uNodeMap,
        uEdgeMap
      )(algebra)
    }
  }
}
