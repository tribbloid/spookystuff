package com.tribbloids.spookystuff.graph

import com.github.mdr.ascii.graph.Graph
import com.github.mdr.ascii.layout.GraphLayout
import com.github.mdr.ascii.layout.prefs.LayoutPrefsImpl
import com.tribbloids.spookystuff.graph.Visualisation.Format

import scala.collection.mutable

case class Visualisation[D <: Domain](
    core: DSL[D]#Core,
    format: Format[D] = Format[D]()
) extends Algebra.Sugars[D] {

  final override def algebra: Algebra[D] = core.algebra

  object Tree {

    //todo: merge following 2
    def showForward(
        startingFrom: Seq[Element[D]]
    ): String = {
      startingFrom
        .map { elem =>
          val view = core._ElementView.fromElement(elem).WFormat(format)
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
          val stepView = core._ElementView.fromElement(elem).WFormat(format)
          val treeNode = stepView.BackwardTreeNode()
          treeNode.treeString(format.verbose)
        }
        .mkString("")
    }
  }

  object ASCIIArt {

    protected final val layoutPrefs = LayoutPrefsImpl(unicode = true, explicitAsciiBends = false)

    def compile(
        endWith: Seq[Element[D]]
    ): Graph[ElementView[D]#WFormat] = {

      val buffer = mutable.HashSet.empty[ElementView[D]#WFormat]
      val relationBuffer = mutable.HashSet.empty[(ElementView[D]#WFormat, ElementView[D]#WFormat)]

      for (elem <- endWith) {
        val stepView = core._ElementView.fromElement(elem).WFormat(format)
        val treeNode = stepView.BackwardTreeNode()
        treeNode.foreach { v =>
          buffer += v.viewWFormat
          v.children.foreach { child =>
            relationBuffer += child.viewWFormat -> v.viewWFormat
          }
        }
      }

      val graph = Graph[ElementView[D]#WFormat](buffer.toSet, relationBuffer.toList)
      graph
    }

    def showASCIIArt(
        forward: Boolean
    ): String = {

      val graph = compile(core.heads.seq)

      val forwardStr = GraphLayout.renderGraph(graph, layoutPrefs = layoutPrefs)
      if (forward) forwardStr
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
      forward: Boolean = true,
      asciiArt: Boolean = false
  ): String = {

    if (!asciiArt) {
      if (forward) {

        val facets = core.dsl.facets
        val strs = facets.map { facet =>
          facet.arrow + "\n" + Tree.showForward(core.tails(facet).seq)
        }

        strs.mkString("")
      } else {
        Tree.showBackward(core.heads.seq)
      }
    } else {
      ASCIIArt.showASCIIArt(forward)
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

  val toStrFn = { v: Any =>
    "" + v
  }

  //TODO: merge into algebra?
  case class Format[T <: Domain](
      _showNode: Element.NodeLike[T] => String = toStrFn,
      _showEdge: Element.Edge[T] => String = toStrFn,
      nodeShortName: Element.NodeLike[T] => String = { v: Element.NodeLike[T] =>
        v.idStr
      },
      edgeShortName: Element.Edge[T] => String = { v: Element.Edge[T] =>
        v.idStr
      },
      showPrefix: Boolean = true,
      verbose: Boolean = false
  ) {

    def showNode(v: Element.NodeLike[T]): String =
      if (v.isDangling) "??"
      else _showNode(v)

    def shortNameOf(v: Element[T]): String = v match {
      case nn: Element.NodeLike[T] => nodeShortName(nn)
      case ee: Element.Edge[T]     => edgeShortName(ee)
    }

    def showElement(v: Element[T]): String = v match {
      case nn: Element.NodeLike[T] => showNode(nn)
      case ee: Element.Edge[T]     => _showEdge(ee)
    }
  }
}
