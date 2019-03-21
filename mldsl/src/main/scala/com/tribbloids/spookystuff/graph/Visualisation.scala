package com.tribbloids.spookystuff.graph

import com.github.mdr.ascii.graph.Graph
import com.github.mdr.ascii.layout.GraphLayout
import com.github.mdr.ascii.layout.prefs.LayoutPrefsImpl
import com.tribbloids.spookystuff.graph.Visualisation.Format

import scala.collection.mutable

case class Visualisation[I <: Impl](
    core: DSL[I]#Core,
    format: Format[I#DD] = Format[I#DD]()
) extends Impl.Sugars[I] {

  final override def algebra: Algebra[DD] = core.algebra

  object Tree {

    //todo: merge following 2
    def showForward(
        startingFrom: Seq[Element[DD]]
    ): String = {
      startingFrom
        .map { ee =>
          val view = core._ElementView.fromElement(ee, format)
          val treeNode = view.ForwardTreeNode()
          treeNode.treeString(format.verbose)
        }
        .mkString("")
    }

    def showBackward(
        endWith: Seq[Element[DD]]
    ): String = {
      endWith
        .map { ee =>
          val stepView = core._ElementView.fromElement(ee, format)
          val treeNode = stepView.BackwardTreeNode()
          treeNode.treeString(format.verbose)
        }
        .mkString("")
    }
  }

  object ASCIIArt {

    protected final val layoutPrefs = LayoutPrefsImpl(unicode = true, explicitAsciiBends = false)

    def compile(
        endWith: Seq[Element[DD]]
    ): Graph[_ElementView] = {

      val buffer = mutable.HashSet.empty[_ElementView]
      val relationBuffer = mutable.HashSet.empty[(_ElementView, _ElementView)]

      for (ee <- endWith) {
        val stepView = core._ElementView.fromElement(ee, format)
        val treeNode = stepView.BackwardTreeNode()
        treeNode.foreach { v =>
          buffer += v.view
          v._children.foreach { child =>
            relationBuffer += child -> v.view
          }
        }
      }

      val graph = Graph[_ElementView](buffer.toSet, relationBuffer.toList)
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
  }
}
