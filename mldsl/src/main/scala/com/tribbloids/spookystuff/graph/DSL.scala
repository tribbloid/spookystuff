package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.graph.DSL.Facet
import com.tribbloids.spookystuff.graph.IDAlgebra.Rotator
import com.tribbloids.spookystuff.graph.Module.{Heads, Tails}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

trait DSL[I <: Impl] extends Impl.Sugars[I] {

  def impl: StaticGraph.Builder[I#DD, I#GProto]
  final override val algebra: Algebra[I#DD] = impl.algebra

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

    lazy val empty = Core(impl.fromSeq(Nil, Nil))
  }

  case class Core(
      self: _Module,
      //tails or heads that doesn't belong to edges in self is tolerable
      tails: Map[Facet, _Tails] = Map.empty,
      heads: _Heads = Heads[DD](),
      fromOverride: Option[_Heads] = None
  ) extends algebra._Sugars {

    val dsl: DSL.this.type = DSL.this

    /**
      * kind of a sanity check
      */
    val _graph = impl.fromModule(self)

    val _graph_WHeadsAndTails = {

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

      val latentGraph: I#GProto[I#DD] = impl.fromSeq(Seq(algebra.DANGLING), (latentTails ++ latentHeads).distinct)

      impl.union(_graph, latentGraph)
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
        val result = impl.union(_graph: I#GProto[I#DD], peer._graph: I#GProto[I#DD])

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

        val (newGraph, conversion) = impl.merge(
          base._graph -> base.from,
          top._graph -> topTails
        )

        val newTails = Map(
          topFacet -> base.tails(topFacet).convert(conversion),
          baseFacet -> top.tails(baseFacet).convert(conversion)
        )
        val newHeads = top.heads.convert(conversion)

        Core(
          newGraph: StaticGraph[I#DD],
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
          self.Ops(topFacet, baseFacet)._mergeImpl(top.replicate(Mutator.replicate[DD])(rotator), tail)
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

    object _ElementView {

      // add constructor from id

      def fromElement(
          element: _Element,
//          neighbourID: Option[ID] = None,
          format: _ShowFormat = _ShowFormat[DD]()
      ): _ElementView = {

        element match {
          case nn: _NodeLike => NodeView(nn.toLinked(Some(Core.this._graph_WHeadsAndTails)), format)
          case ee: _Edge     => EdgeView(ee, format)
        }
      }
    }

    trait _ElementView extends ElementView[I] {

      override val core: Core = Core.this
    }

    case class NodeView(
        linkedNode: _LinkedNode,
        override val format: _ShowFormat = _ShowFormat[DD]()
    ) extends _ElementView {

      import format._

      override def element: _LinkedNode = linkedNode

      def getEdgeViews(idParis: Seq[(ID, ID)]): Seq[EdgeView] = {

        _graph_WHeadsAndTails
          .getEdges(idParis)
          .values
          .flatten
          .toSeq
          .map { v =>
            EdgeView(v, format)
          }
      }

      override def inbound: Seq[EdgeView] = {

        getEdgeViews(linkedNode.inboundIDPairs)
      }

      override def outbound: Seq[EdgeView] = {

        getEdgeViews(linkedNode.outboundIDPairs)
      }

      override lazy val toString: String = showNode(linkedNode.node)
    }

    case class EdgeView(
        edge: _Edge,
        override val format: _ShowFormat = _ShowFormat[DD]()
    ) extends _ElementView {

      import format._

      override def element: _Edge = edge

      def getNodeViews(ids: Seq[ID]) = {
        _graph_WHeadsAndTails
          .getLinkedNodes(ids)
          .values
          .filterNot(_.isDangling)
          .toSeq
          .map { v =>
            NodeView(v, format)
          }
      }

      override def inbound: Seq[NodeView] = {

        getNodeViews(Seq(edge.from))
      }

      override def outbound: Seq[NodeView] = {

        getNodeViews(Seq(edge.to))
      }

      lazy val prefixes: Seq[String] =
        if (showPrefix) {
          val buffer = ArrayBuffer[String]()
          if (core.heads.seq contains edge) buffer += "HEAD"

          val facets: Seq[Facet] = dsl.facets

          val tailSuffix = facets
            .flatMap { facet =>
              val ff = facet
              if (core.tails(facet).seq contains edge) Some(facet.feather)
              else None
            }
            .mkString(" ")

          val tailOpt =
            if (tailSuffix.isEmpty) Nil
            else Seq("TAIL" + tailSuffix)

          buffer ++= tailOpt

          buffer
        } else Nil

      override lazy val toString: String = prefixes.map("(" + _ + ")").mkString("") + " [ " +
        _showEdge(edge) + " ]"
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

    def visualise(format: Visualisation.Format[DD] = defaultFormat): Visualisation[I] =
      Visualisation[I](core, format)
  }

  object Formats {

    object Default extends Visualisation.Format[DD]()

    object ShowData
        extends Visualisation.Format[DD](
          _showNode = { v =>
            "" + v.data
          },
          _showEdge = { v =>
            "" + v.data
          }
        )
  }

  lazy val defaultFormat: Visualisation.Format[DD] = Formats.Default
}

object DSL {

  abstract class Facet(
      val feather: String,
      val arrow: String
  ) {}
}
