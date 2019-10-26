package com.tribbloids.spookystuff.graph

import com.github.mdr.ascii.graph.Graph
import com.github.mdr.ascii.layout.GraphLayout
import com.github.mdr.ascii.layout.prefs.LayoutPrefsImpl
import com.tribbloids.spookystuff.graph.Visualisation.Format
import com.tribbloids.spookystuff.utils.CommonUtils

import scala.collection.mutable

case class Visualisation[D <: Domain](
    core: Layout[D]#Core[Module[D]],
    format: Format[D]
) extends Algebra.Sugars[D] {

  final override def algebra: Algebra[D] = core.algebra

  trait Impl {

    def show: String

    def apply(): String = show
  }

  object Tree extends Impl {

    //todo: merge following 2
    def showForward(
        startingFrom: Seq[Element[D]]
    ): String = {
      startingFrom
        .map { elem =>
          val view = core.Views.fromElement(elem).WFormat(format)
          val treeNode = view.ForwardTreeNode()
          treeNode.treeString(format.verbose)
        }
        .mkString("")
    }

    def showBackward(
        endWith: Seq[Element[D]]
    ): String = {
      endWith
        .map { elem =>
          val stepView = core.Views.fromElement(elem).WFormat(format)
          val treeNode = stepView.BackwardTreeNode()
          treeNode.treeString(format.verbose)
        }
        .mkString("")
    }

    override lazy val show: String = {

      if (format.forward) {

        val facets = core.layout.facetsSorted
        val strs = facets.map { facet =>
          facet.arrow + "\n" + Tree.showForward(core.tails(facet).seq)
        }

        strs.mkString("")
      } else {
        Tree.showBackward(core.heads.seq)
      }
    }
  }

  object ASCIIArt extends Impl {

    def compileASCII(
        endWith: Seq[Element[D]]
    ): Graph[ElementView[D]#WFormat] = {

      val buffer = mutable.LinkedHashSet.empty[ElementView[D]#WFormat]
      val relationBuffer = mutable.LinkedHashSet.empty[(ElementView[D]#WFormat, ElementView[D]#WFormat)]

      for (elem <- endWith) {
        val stepView = core.Views.fromElement(elem).WFormat(format)
        val treeNode = stepView.BackwardTreeNode()

        treeNode.foreach { v =>
          buffer += v.viewWFormat
          //          println(v.viewWFormat)
          //          val vv = v.children.map(_.view.element.dataStr)

          val children = v.children.reverse

          children.foreach { child =>
            buffer += child.viewWFormat
            relationBuffer += child.viewWFormat -> v.viewWFormat
          }
        }
      }

//      buffer.foreach { v =>
//        println(v)
//      }

      val graph = Graph[ElementView[D]#WFormat](buffer.toSet, relationBuffer.toList)
      graph
    }

    override lazy val show: String = {

      val graph = compileASCII(core.heads.seq)

      val forwardStr = GraphLayout.renderGraph(graph, layoutPrefs = format.asciiLayout)
      if (format.forward) forwardStr
      else {
        forwardStr
          .split('\n')
          .reverse
          .mkString("\n")
          .map(Visualisation.flipChar)
      }
    }
  }

  def show(
      asciiArt: Boolean = false
  ): String = {

    if (!asciiArt) {
      Tree.show
    } else {
      ASCIIArt.show
    }
  }
}

object Visualisation {

  final val mirrorImgs = List(
    'v' -> '^',
    '┌' -> '└',
    '┘' -> '┐',
    '┬' -> '┴'
  )

  protected def flipChar(char: Char): Char = {
    mirrorImgs.find(_._1 == char).map(_._2).getOrElse {
      mirrorImgs.find(_._2 == char).map(_._1).getOrElse {
        char
      }
    }
  }

  lazy val defaultASCIILayout = LayoutPrefsImpl(
    explicitAsciiBends = true
  )

  //TODO: merge into algebra?
  case class Format[D <: Domain](
      showNode: Element.NodeLike[D] => String = CommonUtils.toStrNullSafe _,
      showEdge: Element.Edge[D] => String = CommonUtils.toStrNullSafe _,
      nodeShortName: Element.NodeLike[D] => String = { v: Element.NodeLike[D] =>
        v.idStr
      },
      edgeShortName: Element.Edge[D] => String = { v: Element.Edge[D] =>
        v.idStr
      },
      showPrefix: Boolean = true,
      verbose: Boolean = false,
      forward: Boolean = true,
      asciiLayout: LayoutPrefsImpl = defaultASCIILayout
  ) {

    def _showNode(v: Element.NodeLike[D]): String =
      if (v.isDangling) "??"
      else showNode(v)

    def shortNameOf(v: Element[D]): String = v match {
      case nn: Element.NodeLike[D] => nodeShortName(nn)
      case ee: Element.Edge[D]     => edgeShortName(ee)
    }

    def showElement(v: Element[D]): String = v match {
      case nn: Element.NodeLike[D] => _showNode(nn)
      case ee: Element.Edge[D]     => showEdge(ee)
    }
  }
}
