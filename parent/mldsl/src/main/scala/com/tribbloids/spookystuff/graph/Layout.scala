package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator
import com.tribbloids.spookystuff.graph.Layout.Facet
import com.tribbloids.spookystuff.graph.Module.{Heads, Tails}
import com.tribbloids.spookystuff.utils.Caching.ConcurrentMap

import scala.language.implicitConversions

trait Layout[D <: Domain] extends Algebra.Aliases[D] {

  val defaultGraphBuilder: StaticGraph.Builder[D]
  final override val algebra: Algebra[D] = defaultGraphBuilder.algebra

  type GG = defaultGraphBuilder.GG

  def facets: Set[Facet]

  def facetsSorted: Seq[Facet] = facets.toSeq.sortBy(_.positioning)

  object Core {

    def fromElement[T <: _Element](element: T): Core[T] = {
      element match {
        case v: _NodeLike =>
          val tails = v.asTails

          val tailsMap = Map(
            facetsSorted.map { facet =>
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
            facetsSorted.map { facet =>
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

    def Edge(v: EdgeData): Core[_Edge] = {
      val edge = algebra.createEdge(v)
      fromElement(edge)
    }

    def Node(v: NodeData): Core[_Node] = {
      val node = algebra.createNode(v)
      fromElement(node)
    }

    lazy val empty: Core[defaultGraphBuilder.GG] = Core(defaultGraphBuilder.fromSeq(Nil, Nil))
  }

  case class Core[+M <: _Module](
      self: M,
      // tails or heads that doesn't belong to edges in self is tolerable
      tails: Map[Facet, _Tails] = Map.empty,
      heads: _Heads = Heads[D](),
      fromOverride: Option[_Heads] = None
  ) extends algebra._Aliases {

    def layout: Layout.this.type = Layout.this

    lazy val _graph: GG = defaultGraphBuilder.fromModule(self)

    /**
      * kind of a sanity check
      */
    lazy val _graph_WHeadsAndTails: defaultGraphBuilder.GG = {

      val latentTails = tails.values.toList.flatMap(_.seq).distinct
      val latentHeads = heads.seq

      val latentIDs = (latentTails.map(_.to) ++ latentHeads.map(_.from)).distinct
        .filterNot(_ == algebra.DANGLING._equalBy)

      val existingIDs = _graph.getLinkedNodes(latentIDs).keys.toList

      require(
        existingIDs.size == latentIDs.size,
        s"some of the node ID(s) in heads or tails doesn't exist. " +
          s"Required: ${latentIDs.mkString(", ")}, " +
          s"Existing: ${existingIDs.mkString(", ")}"
      )

      val latentGraph =
        defaultGraphBuilder.fromSeq(Seq(algebra.DANGLING), (latentTails ++ latentHeads).distinct)

      defaultGraphBuilder.union(
        _graph,
        latentGraph,
        { (v1, _) =>
          v1 // nodes in latentGraph should have incorrect data
        }
      )
    }

    lazy val from: _Heads = fromOverride.getOrElse(heads)

    def replicate(m: DataMutator = DataAlgebra.Mutator.identity)(
        implicit
        idRotator: Rotator[ID]
    ): Core[M] = {

      this.copy(
        self.replicate(m),
        tails.mapValues(_.replicate(m)).toMap,
        heads.replicate(m)
      )
    }

    def canConnectFrom(v: Facet): Boolean = {
      tails(v).seq.nonEmpty
    }

    private def baseM = this

    case class Ops(
        topFacet: Facet,
        baseFacet: Facet
    ) {

      def union(peer: Core[_]): Core[GG] = {
        val result = defaultGraphBuilder.union(_graph: GG, peer._graph: GG)

        val tails = {
          val facets = (baseM.tails.keys.toSeq ++ peer.tails.keys.toSeq).distinct
          Map(
            facets.map { facet =>
              val tails = baseM.tails(facet) ++ peer.tails(facet)
              facet -> tails
            }: _*
          )
        }

        val heads = baseM.heads ++ peer.heads

        Core(result, tails, heads)
      }

      //      def checkConnectivity(top: DSLView, topTails: _Tails): DSLView = {}

      def _mergeImpl(top: Core[_], topTails: _Tails): Core[GG] = {

        val (newGraph, conversion) = defaultGraphBuilder.serial(
          baseM._graph -> baseM.from,
          top._graph -> topTails
        )

        val newTails = Map(
          topFacet -> baseM.tails(topFacet).convert(conversion),
          baseFacet -> top.tails(baseFacet).convert(conversion)
        )
        val newHeads = top.heads.convert(conversion)

        Core(
          newGraph: GG,
          newTails,
          newHeads
        )
      }

      def compose(top: Core[_]): Core[GG] = {

        _mergeImpl(top, top.tails(topFacet))
      }

      def mapHead(top: Core[_]): Core[_Module] = {

        val topTails: Seq[Element.Edge[D]] = top.tails(baseFacet).seq
        val rotatorFactory = idAlgebra.rotatorFactory()

        topTails.foldLeft(baseM: Core[_Module]) { (self, edge) =>
          val tail = Tails(Seq(edge))
          val rotator = rotatorFactory()
          self.Ops(topFacet, baseFacet)._mergeImpl(top.replicate(DataAlgebra.Mutator.identity[D])(rotator), tail)
        }
      }

      // this is really kind of ambiguous, remove it?
      def append(top: Core[_]): Core[_Module] = {

        val intakes = top.tails
        assert(intakes.size <= 1, "non-linear right operand, please use merge, rebase or union instead")
        intakes.headOption match {
          case Some(_) =>
            this.mapHead(top)
          case _ =>
            this.union(top)
        }
      }
    }

    trait _ElementView extends ElementView[D] {

      override val core: Core[M] = Core.this
    }

    object Views {
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
          case nn: _NodeLike => fromNode(nn)
          case ee: _Edge     => fromEdge(ee)
        }
      }

      def fromEdge(ee: _Edge): EdgeView = {
        val existingSeq = fromEdgeIDs(ee.from_to)

        existingSeq
          .find(_.element == ee)
          .getOrElse(EdgeView(ee))
      }

      def fromNode(nn: _NodeLike): NodeView = {
        val existingOpt = fromNodeID(nn._equalBy)
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
      }
    }

    case class NodeView(
        linkedNode: _NodeTriplet
    ) extends _ElementView {

      override def element: _NodeTriplet = linkedNode

      def getEdgeViews(idPairs: Seq[(ID, ID)]): Seq[EdgeView] = {

        idPairs.flatMap { ids =>
          Views.fromEdgeIDs(ids)
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

      // TODO: add operation to render all lazy val eagerly
    }

    case class EdgeView(
        edge: _Edge
    ) extends _ElementView {

      override def element: _Edge = edge

      def getNodeViews(ids: Seq[ID]): Seq[NodeView] = {
        ids
          .filter(_ != algebra.idAlgebra.DANGLING)
          .flatMap { id =>
            Views.fromNodeID(id)
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

  trait DSL {

    def formats: Formats.type = Layout.this.Formats

    type Operand[+M <: _Module] <: OperandLike[M]

    type Op = Operand[_Module]

    trait OperandLike[+M <: _Module] {

      final def outerDSL: DSL = DSL.this

      def core: Core[M]
      final def self: M = core.self

      def output: Core[_Module] = core

      def visualise(format: Visualisation.Format[D] = defaultFormat): Visualisation[D] =
        Visualisation[D](output, format)

      def from(filter: EdgeFilter[D]): Operand[M] = {
        val newFrom: _Heads = Heads[D](filter(core._graph))
        create(
          core.copy(
            fromOverride = Some(newFrom)
          )
        )
      }

      // TODO: these symbols are lame
      final def >-[N >: M <: _Module](filter: EdgeFilter[D]): Operand[M] = from(filter)

      // TODO: should be part of EdgeFilter
      def and(filter: EdgeFilter[D]): Operand[M] = {
        val newFrom = Heads[D](filter(core._graph))
        val mergedFrom = Heads[D](core.from.seq ++ newFrom.seq)
        create(
          core.copy(
            fromOverride = Some(mergedFrom)
          )
        )
      }

      final def <>-(filter: EdgeFilter[D]): Operand[M] = and(filter)

      def replicate(m: DataMutator = DataAlgebra.Mutator.identity)(
          implicit
          idRotator: Rotator[ID]
      ): Operand[M] = {
        create(
          core.replicate(m)(idRotator)
        )
      }
    }

    def create[M <: _Module](core: Core[M]): Operand[M]
    final def create[M <: _Element](element: M): Operand[M] = create(Core.fromElement(element))

    def Edge(v: EdgeData): Operand[_Edge] = create(Core.Edge(v))
    def Node(v: NodeData): Operand[_Node] = create(Core.Node(v))

    sealed trait Implicits_Level1 {

      implicit def Edge(v: EdgeData): Operand[_Edge] = DSL.this.Edge(v)
    }

    object Implicits extends Implicits_Level1 {

      implicit def Node(v: NodeData): Operand[_Node] = DSL.this.Node(v)
    }
  }

  object Formats {

    object Default extends Visualisation.Format[D]()

    object ShowData
        extends Visualisation.Format[D](
          showNode = { v =>
            "" + v.data
          },
          showEdge = { v =>
            "" + v.data
          }
        )

    object ShowOption
        extends Visualisation.Format[D](
          showNode = { v =>
            Layout.monad2Str(v.data)
          },
          showEdge = { v =>
            Layout.monad2Str(v.data)
          }
        )
  }

  lazy val defaultFormat: Visualisation.Format[D] = Formats.Default
}

object Layout {

  abstract class Facet(
      val feather: String,
      val arrow: String
  ) {

    override def toString: String = arrow

    def positioning: Int = 0
  }

  private def monad2Str(v: Any) = {
    v match {
      case Some(vv) => "" + vv
      case None     => "∅"
      case _        => "" + v
    }
  }
}
