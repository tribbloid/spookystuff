package com.tribbloids.spookystuff.graph

import ai.acyclic.prover.commons.same.EqualBy
import com.tribbloids.spookystuff.graph.Layout.Facet

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

object ElementView {

  implicit def ordering[D <: Domain]: Ordering[ElementView[D]] = {

    Ordering.by { (v: ElementView[D]) =>
      v.orderedBy
    } // (Ordering.Tuple3[Iterable[Int], Iterable[String], String])
  }
}

trait ElementView[D <: Domain] extends Algebra.Aliases[D] with EqualBy {

  val core: Layout[D]#Core[?]
  final override def algebra: Algebra[D] = core.algebra

  def element: _Element

  def inbound: Seq[ElementView[D]]
  def outbound: Seq[ElementView[D]]

  final override def samenessKey: _Element = element

  lazy val (prefixes, positioning): (Seq[String], Seq[Int]) = {
    element match {
      case edge: Element.Edge[D] =>
        val prefixes = ArrayBuffer[String]()
        val positioning = ArrayBuffer[Int]()

        if (core.heads.seq contains edge) {
          prefixes += "HEAD"
          positioning += -1
        }

        val facets: Seq[Facet] = core.layout.facetsSorted

        val feathers: Seq[String] = facets
          .flatMap { facet =>
            if (core.tails(facet).seq contains edge) {
              positioning += facet.positioning
              Seq(facet.feather)
            } else Nil
          }

        val tailOpt =
          if (feathers.isEmpty) Nil
          else Seq("TAIL" + feathers.mkString(" "))

        prefixes ++= tailOpt
        if (positioning.isEmpty) positioning += 0

        prefixes.toSeq -> positioning.toSeq
      case _ =>
        Nil -> Nil
    }
  }

  lazy val orderedBy: (Iterable[Int], Iterable[String], String) = {

    (
      positioning,
      prefixes,
      element.dataStr
    )
  }

  case class WFormat(
      format: _ShowFormat
  ) {

    def outer: ElementView[D] = ElementView.this

    override lazy val toString: String = {
      val _prefix: Option[String] = if (format.showPrefix && prefixes.nonEmpty) {
        Some(prefixes.map("(" + _ + ")").mkString(""))
      } else None

      val _body = element match {
        case edge: Element.Edge[D] =>
          "[ " + format.showEdge(edge) + " ]"
        case node: Element.NodeLike[D] =>
          format.showNode(node)
        case v @ _ =>
          "" + v
      }

      (_prefix.toSeq ++ Seq(_body)).mkString(" ")
    }

    case class ForwardTreeNode(
        // VERY IMPORTANT for cycle elimination
        visited: Set[_Element] = Set.empty
    ) extends ElementTreeNode[D] {

      override def viewWFormat: ElementView[D]#WFormat = WFormat.this

      override def dirSymbol: String = "v "

      override val _children: Seq[ElementView[D]] = ElementView.this.outbound

      implicit override def copyImplicitly(v: ElementView[D]): ElementTreeNode[D] =
        v.WFormat(format).ForwardTreeNode(visited + element)
    }

    case class BackwardTreeNode(
        // VERY IMPORTANT for cycle elimination
        visited: Set[_Element] = Set.empty
    ) extends ElementTreeNode[D] {

      override def viewWFormat: ElementView[D]#WFormat = WFormat.this

      override def dirSymbol: String = "^ "

      override val _children: Seq[ElementView[D]] = ElementView.this.inbound

      implicit override def copyImplicitly(v: ElementView[D]): ElementTreeNode[D] =
        v.WFormat(format).BackwardTreeNode(visited + element)
    }
  }
}
