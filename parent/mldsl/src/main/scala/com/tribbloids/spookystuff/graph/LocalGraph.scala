package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.Element.{Edge, NodeTriplet}
import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator
import com.tribbloids.spookystuff.utils.{CommonTypes, MultiMapView}

import scala.collection.mutable

//optimised for speed rather than memory usage
//NOT thread safe!
//
//TODO: this may leverage an existing java/scala graph library
//TODO: also need SparkGraphImpl, use GraphX or GraphFrame depending on their maturity
case class LocalGraph[D <: Domain] private (
    nodeMap: mutable.Map[D#ID, NodeTriplet[D]],
    edgeMap: MultiMapView.Mutable[(D#ID, D#ID), Edge[D]]
)(
    implicit
    override val algebra: Algebra[D]
) extends StaticGraph[D] {

  override def _replicate(m: DataMutator)(
      implicit
      idRotator: Rotator[ID],
      node_+ : CommonTypes.Binary[NodeData]
  ): LocalGraph[D] = {

    new LocalGraph.BuilderImpl[D]()
      .fromSeq(
        nodeMap.values.toSeq.map(_.replicate(m)),
        edgeMap.values.toSeq.flatMap(_.map(_.replicate(m))),
        node_+
      )
  }

  def removeIfExists_![T](set: mutable.Set[T], v: T): Unit = {

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
        removeIfExists_!(node.outbound, edge.to)
      }

      nodeMap.get(edge.to).foreach { node =>
        removeIfExists_!(node.inbound, edge.from)
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

  override def getLinkedNodes(ids: Seq[ID]): Map[ID, _NodeTriplet] =
    Map(ids.flatMap { id =>
      nodeMap.get(id).map(id -> _)
    }: _*)

  override def getEdges(ids: Seq[(ID, ID)]): MultiMapView.Immutable[(ID, ID), _Edge] =
    Map(ids.flatMap { pair =>
      edgeMap.get(pair).map(pair -> _)
    }: _*)
}

object LocalGraph {

  class BuilderImpl[D <: Domain](
      implicit
      override val algebra: Algebra[D]
  ) extends StaticGraph.Builder[D] {

    type GG = LocalGraph[D]
    override def ctg = getCtg

    override def fromSeq(
        nodes: Seq[_NodeLike],
        edges: Seq[_Edge],
        node_+ : CommonTypes.Binary[NodeData]
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

      val linkedNodes: Seq[_NodeTriplet] = _nodes.map {
        case nn: _NodeTriplet =>
          nn
        case nn: _Node =>
          val inbound = mutable.LinkedHashSet(edges.filter(_.to == nn._id).map(_.from): _*)
          val outbound = mutable.LinkedHashSet(edges.filter(_.from == nn._id).map(_.to): _*)
          val result = new _NodeTriplet(nn, inbound, outbound)
          result
      }

      val nodeMap: MultiMapView.Mutable[ID, _NodeTriplet] = MultiMapView.Mutable.empty
      for (nn <- linkedNodes) {
        nodeMap.put1(nn._id, nn)
      }

      val reducedNodeMap: mutable.Map[ID, _NodeTriplet] = {

        val result = nodeMap.mapValues { seq =>
          seq.reduce { (v1, v2) =>
            linkedNode_+(v1, v2, node_+)
          }
        }
        mutable.Map(result.toSeq: _*)
      }

      val edgeMap: MultiMapView.Mutable[(ID, ID), _Edge] = MultiMapView.Mutable.empty
      for (ee <- edges) {
        edgeMap.put1(ee.from_to, ee)
      }

      new GG(
        reducedNodeMap,
        edgeMap
      )(algebra)
    }

    protected def linkedNode_+(
        v1: _NodeTriplet,
        v2: _NodeTriplet,
        node_+ : CommonTypes.Binary[NodeData]
    ): _NodeTriplet = {

      require(v1._id == v2._id, s"ID mismatch, ${v1._id} ~= ${v2._id}")

      val node = algebra.createNode(node_+(v1.data, v2.data), Some(v1._id))

      val inbound = v1.inbound ++ v2.inbound
      val outbound = v1.outbound ++ v2.outbound
      new _NodeTriplet(node, inbound, outbound)
    }

    // TODO: using fromSeq can make it trivial!
    def union(
        v1: GG,
        v2: GG,
        node_+ : CommonTypes.Binary[NodeData]
    ): GG = {

      val v2Reduced: mutable.Map[D#ID, _NodeTriplet] = v2.nodeMap.map {
        case (k, vv2) =>
          val reducedV: _NodeTriplet = v1.nodeMap
            .get(k)
            .map { vv1 =>
              linkedNode_+(vv1, vv2, node_+)
            }
            .getOrElse(vv2)
          k -> reducedV
      }

      val uNodeMap: mutable.Map[ID, _NodeTriplet] = v1.nodeMap ++ v2Reduced
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
