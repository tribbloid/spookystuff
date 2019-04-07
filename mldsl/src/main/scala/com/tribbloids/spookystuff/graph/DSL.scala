package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.DSL.Facet
import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator
import com.tribbloids.spookystuff.graph.Module.{Heads, Tails}
import com.tribbloids.spookystuff.utils.CachingUtils.ConcurrentMap

import scala.language.implicitConversions

trait DSL[D <: Domain] extends Algebra.Sugars[D] {

  val defaultGraphBuilder: StaticGraph.Builder[D]
  final override val algebra: Algebra[D] = defaultGraphBuilder.algebra

  type GG = defaultGraphBuilder.GG

  def facets: List[Facet]

  object Core {

    def fromElement(element: _Element): Core = {
      element match {
        case v: _NodeLike =>
          val tails = v.asTails

          val tailsMap = Map(
            facets.map { facet =>
              facet ->
                tails.copy(
                  tails.seq.map(_.copy(qualifier = Seq(facet))(algebra))
                )
            }: _*
          )

          val heads = v.asHeads

          Core(
            element,
            tailsMap,
            heads
          )

        case v: _Edge =>
          val tails = v.asTails
          val tailsMap = Map(
            facets.map { facet =>
              facet -> tails
            }: _*
          )

          val heads = v.asHeads

          Core(
            element,
            tailsMap,
            heads
          )
      }

    }

    def fromEdgeData(v: EdgeData): Core = {
      val node = algebra.createEdge(v)
      fromElement(node)
    }

    def fromNodeData(v: NodeData): Core = {
      val node = algebra.createNode(v)
      fromElement(node)
    }

    lazy val empty = Core(defaultGraphBuilder.fromSeq(Nil, Nil))
  }

  case class Core(
      self: _Module,
      //tails or heads that doesn't belong to edges in self is tolerable
      tails: Map[Facet, _Tails] = Map.empty,
      heads: _Heads = Heads[D](),
      fromOverride: Option[_Heads] = None
  ) extends algebra._Sugars {

    def dsl: DSL.this.type = DSL.this

    lazy val _graph: GG = defaultGraphBuilder.fromModule(self)

    /**
      * kind of a sanity check
      */
    lazy val _graph_WHeadsAndTails = {

      val latentTails = tails.values.toList.flatMap(_.seq).distinct
      val latentHeads = heads.seq

      val latentIDs = (latentTails.map(_.to) ++ latentHeads.map(_.from)).distinct
        .filterNot(_ == algebra.DANGLING._id)

      val existingIDs = _graph.getLinkedNodes(latentIDs).keys.toList

      require(
        existingIDs.size == latentIDs.size,
        s"some of the node ID(s) in heads or tails doesn't exist. " +
          s"Required: ${latentIDs.mkString(", ")}, " +
          s"Existing: ${existingIDs.mkString(", ")}"
      )

      val latentGraph =
        defaultGraphBuilder.fromSeq(Seq(algebra.DANGLING), (latentTails ++ latentHeads).distinct)

      defaultGraphBuilder.union(_graph, latentGraph)
    }

    lazy val from: _Heads = fromOverride.getOrElse(heads)

    def replicate(m: _Mutator)(implicit idRotator: Rotator[ID]): Core = {

      this.copy(
        self.replicate(m),
        tails.mapValues(_.replicate(m)),
        heads.replicate(m)
      )
    }

    def canConnectFrom(v: Facet): Boolean = {
      tails(v).seq.nonEmpty
    }

    def base: Core = this

    case class Ops(
        topFacet: Facet,
        baseFacet: Facet
    ) {

      def union(peer: Core): Core = {
        val result = defaultGraphBuilder.union(_graph: GG, peer._graph: GG)

        val tails = {
          val facets = (base.tails.keys.toSeq ++ peer.tails.keys.toSeq).distinct
          Map(
            facets.map { facet =>
              val tails = base.tails(facet) ++ peer.tails(facet)
              facet -> tails
            }: _*
          )
        }

        val heads = base.heads ++ peer.heads

        Core(result, tails, heads)
      }

      //      def checkConnectivity(top: DSLView, topTails: _Tails): DSLView = {}

      def _mergeImpl(top: Core, topTails: _Tails): Core = {

        val (newGraph, conversion) = defaultGraphBuilder.merge(
          base._graph -> base.from,
          top._graph -> topTails
        )

        val newTails = Map(
          topFacet -> base.tails(topFacet).convert(conversion),
          baseFacet -> top.tails(baseFacet).convert(conversion)
        )
        val newHeads = top.heads.convert(conversion)

        Core(
          newGraph: StaticGraph[D],
          newTails,
          heads = newHeads
        )
      }

      def merge(top: Core): Core = {

        _mergeImpl(top, top.tails(topFacet))
      }

      def rebase(top: Core): Core = {

        val topTails = top.tails(baseFacet).seq

        val rotatorFactory = idAlgebra.rotatorFactory()

        topTails.foldLeft(Core.this) { (self, edge) =>
          val tail = Tails(Seq(edge))
          val rotator = rotatorFactory()
          self.Ops(topFacet, baseFacet)._mergeImpl(top.replicate(Mutator.replicate[D])(rotator), tail)
        }
      }

      //this is really kind of ambiguous, remove it?
      def commit(top: Core): Core = {

        val intakes = top.tails
        assert(intakes.size <= 1, "non-linear right operand, please use merge, rebase or union instead")
        intakes.headOption match {
          case Some(intake) =>
            this.rebase(top)
          case _ =>
            this.union(top)
        }
      }
    }

    trait _ElementView extends ElementView[D] {

      override val core: Core = Core.this
    }

    object _ElementView {

      object Cache {
        // compiled once and used all the time
        // may obsolete as underlying Core._graph is mutable, but any attempt to fix this is overengineering
        @transient lazy val nodeViews: ConcurrentMap[ID, Option[NodeView]] = ConcurrentMap()
        @transient lazy val edgeViews: ConcurrentMap[(ID, ID), Seq[EdgeView]] = ConcurrentMap()
      }

      // add constructor from id
      def fromNodeID(id: ID): Option[NodeView] = {
        Cache.nodeViews.getOrElse(
          id,
          Core.this.synchronized {
            val nodeOpt = Core.this._graph_WHeadsAndTails.getLinkedNodes(Seq(id)).values.headOption

            val result = nodeOpt.map { node =>
              NodeView(node)
            }

            if (result.nonEmpty) Cache.nodeViews += id -> result

            result
          }
        )
      }

      def fromEdgeIDs(ids: (ID, ID)): Seq[EdgeView] = {
        Cache.edgeViews.getOrElse(
          ids,
          Core.this.synchronized {
            val edgeSeq = Core.this._graph_WHeadsAndTails.getEdges(Seq(ids)).values.toSeq.flatten

            val result: Seq[EdgeView] = edgeSeq.map { edge =>
              EdgeView(edge)
            }

            if (result.nonEmpty) Cache.edgeViews += ids -> result

            result
          }
        )
      }

      def fromElement(element: _Element): _ElementView = {

        element match {
          case nn: _NodeLike =>
            val existingOpt = fromNodeID(nn._id)
            existingOpt match {
              case Some(existing) =>
                assert(
                  existing.element.data == nn.data,
                  s"Node has the same ID ${nn.idStr} but different data ${nn.dataStr}/${existing.element.dataStr}"
                )
                existing
              case None =>
                NodeView(nn.toLinked(Some(Core.this._graph_WHeadsAndTails)))
            }

          case ee: _Edge =>
            val existingSeq = fromEdgeIDs(ee.from_to)

            existingSeq
              .find(_.element == ee)
              .getOrElse(EdgeView(ee))
        }
      }
    }

    case class NodeView(
        linkedNode: _LinkedNode
    ) extends _ElementView {

      override def element: _LinkedNode = linkedNode

      def getEdgeViews(idPairs: Seq[(ID, ID)]): Seq[EdgeView] = {

        idPairs.flatMap { ids =>
          _ElementView.fromEdgeIDs(ids)
        }
      }

      override lazy val inbound: Seq[EdgeView] = {

        getEdgeViews(linkedNode.inboundIDPairs)
      }

      override lazy val outbound: Seq[EdgeView] = {

        getEdgeViews(linkedNode.outboundIDPairs)
      }

      lazy val inbound2x: Seq[(EdgeView, NodeView)] = {
        inbound.flatMap { edgeV =>
          val upstreamNodes = edgeV.inbound

          upstreamNodes.map { node =>
            edgeV -> node
          }
        }
      }

      lazy val outbound2x: Seq[(EdgeView, NodeView)] = {
        outbound.flatMap { edgeV =>
          val downstreamNodes = edgeV.outbound

          downstreamNodes.map { node =>
            edgeV -> node
          }
        }
      }

      //TODO: add operation to render all lazy val eagerly
    }

    case class EdgeView(
        edge: _Edge
    ) extends _ElementView {

      override def element: _Edge = edge

      def getNodeViews(ids: Seq[ID]) = {
        ids
          .filter(_ != algebra.idAlgebra.DANGLING)
          .flatMap { id =>
            _ElementView.fromNodeID(id)
          }
      }

      override lazy val inbound: Seq[NodeView] = {

        getNodeViews(Seq(edge.from))
      }

      override lazy val outbound: Seq[NodeView] = {

        getNodeViews(Seq(edge.to))
      }

    }
  }

  trait InterfaceImplicits_Level1 {

    type Self <: Interface

    implicit def copyImplicitly(core: Core): Self

    implicit def fromEdgeData(v: EdgeData): Self = Core.fromEdgeData(v)

  }

  trait InterfaceImplicits extends InterfaceImplicits_Level1 {
    implicit def fromNodeData(v: NodeData): Self = Core.fromNodeData(v)
  }

  trait Interface extends InterfaceImplicits {

    def core: Core

    def visualise(format: Visualisation.Format[D] = defaultFormat): Visualisation[D] =
      Visualisation[D](core, format)
  }

  object Formats {

    object Default extends Visualisation.Format[D]()

    object ShowData
        extends Visualisation.Format[D](
          _showNode = { v =>
            "" + v.data
          },
          _showEdge = { v =>
            "" + v.data
          }
        )
  }

  lazy val defaultFormat: Visualisation.Format[D] = Formats.Default
}

object DSL {

  abstract class Facet(
      val feather: String,
      val arrow: String
  ) {

    override def toString: String = arrow
  }
}
