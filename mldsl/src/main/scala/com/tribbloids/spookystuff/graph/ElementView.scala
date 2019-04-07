package com.tribbloids.spookystuff.graph
import com.tribbloids.spookystuff.graph.DSL.Facet
import com.tribbloids.spookystuff.utils.IDMixin

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

trait ElementView[D <: Domain] extends Algebra.Sugars[D] with IDMixin {

  val core: DSL[D]#Core
  override final def algebra: Algebra[D] = core.algebra

  def element: _Element

  def inbound: Seq[ElementView[D]]
  def outbound: Seq[ElementView[D]]

  override final def _id = element

  case class WFormat(
      format: _ShowFormat = _ShowFormat[D]()
  ) {

    def outer: ElementView[D] = ElementView.this

    override lazy val toString: String = element match {
      case edge: Element.Edge[D] =>
        val prefixes: Seq[String] =
          if (format.showPrefix) {
            val buffer = ArrayBuffer[String]()
            if (core.heads.seq contains edge) buffer += "HEAD"

            val facets: Seq[Facet] = core.dsl.facets

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

        prefixes.map("(" + _ + ")").mkString("") + " [ " +
          format._showEdge(edge) + " ]"

      case node: Element.NodeLike[D] =>
        format._showNode(node)
      case v @ _ =>
        "" + v
    }

    case class ForwardTreeNode(
        /**
          * VERY IMPORTANT for cycle elimination
          */
        visited: Set[_Element] = Set.empty
    ) extends ElementTreeNode[D] {

      override def viewWFormat = WFormat.this

      override def dirSymbol = "v "

      override val _children: Seq[ElementView[D]] = ElementView.this.outbound

      override implicit def copyImplicitly(v: ElementView[D]): ElementTreeNode[D] =
        v.WFormat(format).ForwardTreeNode(visited + element)
    }

    case class BackwardTreeNode(
        /**
          * VERY IMPORTANT for cycle elimination
          */
        visited: Set[_Element] = Set.empty
    ) extends ElementTreeNode[D] {

      override def viewWFormat = WFormat.this

      override def dirSymbol = "^ "

      override val _children: Seq[ElementView[D]] = ElementView.this.inbound

      override implicit def copyImplicitly(v: ElementView[D]): ElementTreeNode[D] =
        v.WFormat(format).BackwardTreeNode(visited + element)
    }
  }

}
