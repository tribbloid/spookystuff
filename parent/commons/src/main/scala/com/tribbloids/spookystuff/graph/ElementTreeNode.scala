package com.tribbloids.spookystuff.graph

import com.tribbloids.spookystuff.tree.TreeView

import scala.language.implicitConversions

// technically only StaticGraph is required, DSL is optional, but whatever
//TODO: not optimized, children are repeatedly created when calling .path
//TODO: use mapChildren to recursively get TreeNode[(Seq[String] -> Tree)] efficiently
trait ElementTreeNode[D <: Domain] extends TreeView.Immutable[ElementTreeNode[D]] with Algebra.Aliases[D] {

  def viewWFormat: ElementView[D]#WFormat
  final def view: ElementView[D] = viewWFormat.outer
  final override def algebra: Algebra[D] = view.core.algebra

  def visited: Set[_Element]

  def dirSymbol: String = ""
  lazy val prefix: String = view.element match {
    case _: _NodeLike => ""
    case _: _Edge     => dirSymbol
  }

  val _children: Seq[ElementView[D]]

  implicit def copyImplicitly(v: ElementView[D]): ElementTreeNode[D]

  def copyCutOffCyclic(v: ElementView[D]): ElementTreeNode[D] = {

    val result = copyImplicitly(v)
    val resultElement = result.view.element
    if (visited.contains(resultElement)) ElementTreeNode.Cyclic(result)
    else result
  }

  final override def simpleString(maxFields: Int): String = prefix + viewWFormat.toString

  lazy val paths: Seq[Seq[String]] = {
    val rootPath = Seq(viewWFormat.format.shortNameOf(view.element))
    if (children.nonEmpty) {
      children.flatMap { child =>
        child.paths.map(_ ++ rootPath)
      }
    } else Seq(rootPath)
  }

  lazy val mergedPath: Seq[String] = {

    val numPaths = paths.map(_.size)
    assert(numPaths.nonEmpty, "impossible")
    val result = {
      val maxBranchLength = numPaths.max
      val commonAncestorLength = maxBranchLength
        .to(0, -1)
        .find { v =>
          paths.map(_.slice(0, v)).distinct.size == 1
        }
        .getOrElse(0)

      val commonAncestor = paths.head.slice(0, commonAncestorLength)

      val commonParentLength = maxBranchLength
        .to(0, -1)
        .find { v =>
          paths.map(_.reverse.slice(0, v)).distinct.size == 1
        }
        .getOrElse(0)
      val commonParent = paths.head.reverse.slice(0, commonParentLength).reverse

      if (commonAncestor.size + commonParent.size > maxBranchLength) commonParent
      else commonAncestor ++ commonParent
    }
    result
  }

  override lazy val children: Seq[ElementTreeNode[D]] = {

    val asList = _children.toList.sorted

    val result = asList
      .map {
        copyCutOffCyclic
      }

    result
  }
}

object ElementTreeNode {

  case class Cyclic[D <: Domain](delegate: ElementTreeNode[D]) extends ElementTreeNode[D] {

    override def viewWFormat: ElementView[D]#WFormat = delegate.viewWFormat

    override def visited: Set[_Element] = delegate.visited
    override lazy val prefix: String = delegate.prefix + "(cyclic)"
    override val _children: Seq[ElementView[D]] = Nil

    implicit override def copyImplicitly(v: ElementView[D]): ElementTreeNode[D] = delegate.copyImplicitly(v)
  }
}
