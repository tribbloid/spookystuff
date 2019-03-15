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
      Core(
        element,
        Map.empty,
        element.asTails,
        element.asHeads
      )
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
      private val _tails: Map[Facet, _Tails] = Map.empty,
      defaultTails: _Tails = Tails[DD](), // withDefaultValue is not type safe, thus extra wards
      heads: _Heads = Heads[DD](),
      fromOverride: Option[_Heads] = None
  ) extends algebra._Sugars {

    val dsl: DSL.this.type = DSL.this
    def facets = DSL.this.facets

    lazy val from: _Heads = fromOverride.getOrElse(heads)

    lazy val _graph = impl.fromModule(self)
    lazy val tails: Map[Facet, _Tails] = _tails.withDefaultValue(defaultTails)

    def replicate(m: _Mutator)(implicit idRotator: Rotator[ID]): Core = {

      this.copy(
        self.replicate(m),
        _tails.mapValues(_.replicate(m)),
        defaultTails.replicate(m),
        heads.replicate(m)
      )
    }

    def canConnectFrom(v: Facet): Boolean = {
      tails(v).seq.nonEmpty
    }

    def base: Core = this

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

      val defaultTails = base.defaultTails ++ peer.defaultTails
      val heads = base.heads ++ peer.heads

      Core(result, tails, defaultTails, heads)
    }

    case class Ops(
        topFacet: Facet,
        baseFacet: Facet
    ) {

      //      def checkConnectivity(top: DSLView, topTails: _Tails): DSLView = {}

      def _mergeImpl(top: Core, topTails: _Tails): Core = {

        val result = impl.merge(
          base._graph -> base.from,
          top._graph -> topTails
        )

        val newTails = Map(
          topFacet -> base.tails(topFacet),
          baseFacet -> top.tails(baseFacet)
        )
        val newHeads = top.heads

        Core(
          result: StaticGraph[I#DD],
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
            Core.this.union(top)
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
          case nn: _NodeLike => NodeView(nn.toLinked(Some(Core.this._graph)), format)
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

      override def inbound: Seq[EdgeView] = {

        _graph.getEdges(linkedNode.inboundIDPairs).values.flatten.toSeq.map { v =>
          EdgeView(v, format)
        }
      }

      override def outbound: Seq[EdgeView] = {

        _graph.getEdges(linkedNode.outboundIDPairs).values.flatten.toSeq.map { v =>
          EdgeView(v, format)
        }
      }

      override lazy val toString: String = showNode(linkedNode.node)
    }

    case class EdgeView(
        edge: _Edge,
        override val format: _ShowFormat = _ShowFormat[DD]()
    ) extends _ElementView {

      import format._

      override def element: _Edge = edge

      override def inbound: Seq[NodeView] = {

        _graph.getLinkedNodes(Seq(edge.ids._1)).values.toSeq.map { v =>
          NodeView(v, format)
        }
      }

      override def outbound: Seq[NodeView] = {

        _graph.getLinkedNodes(Seq(edge.ids._2)).values.toSeq.map { v =>
          NodeView(v, format)
        }
      }

      lazy val prefixes: Seq[String] =
        if (showPrefix) {
          val buffer = ArrayBuffer[String]()
          if (core.heads.seq contains edge) buffer += "HEAD"

          val facets: Seq[Facet] = dsl.facets

          val tailSuffix = facets
            .flatMap { facet =>
              val ff = facet
              if (core.tails(facet).seq contains edge) Some(facet.symbol)
              else None
            }
            .mkString(" ")

          val tailStr =
            if (tailSuffix.isEmpty) ""
            else "TAIL" + tailSuffix

          buffer += tailStr

          buffer
        } else Nil

      override lazy val toString: String = prefixes.map("(" + _ + ")").mkString("") + " [ " +
        showEdge(edge) + " ]"
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
          showNode = { v =>
            "" + v.data
          },
          showEdge = { v =>
            "" + v.data
          }
        )
  }

  lazy val defaultFormat: Visualisation.Format[DD] = Formats.Default
}

object DSL {

  abstract class Facet(val symbol: String)
}
