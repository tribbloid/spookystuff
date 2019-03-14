package com.tribbloids.spookystuff.graph

import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.language.implicitConversions

// technically only StaticGraph is required, DSL is optional, but whatever
//TODO: not optimized, children are repeatedly created when calling .path
//TODO: use mapChildren to recursively get TreeNode[(Seq[String] -> Tree)] efficiently
trait ElementTreeNode[I <: Impl] extends TreeNode[ElementTreeNode[I]] with Impl.Sugars[I] {

  def view: ElementView[I]
  final override def algebra: Algebra[I#DD] = view.core.algebra

  def visited: Set[_Element]

  def dirSymbol: String = ""
  lazy val prefix: String = view.element match {
    case _: _NodeLike => ""
    case _: _Edge     => dirSymbol
  }

  val _children: Seq[ElementView[I]]

  implicit def copyImplicitly(v: ElementView[I]): ElementTreeNode[I]

  def copyCutOffCyclic(v: ElementView[I]): ElementTreeNode[I] = {

    val result = copyImplicitly(v)
    val resultElement = result.view.element
    if (visited.contains(resultElement)) ElementTreeNode.Cyclic(result)
    else result
  }

  final override lazy val simpleString: String = prefix + view.toString

  override def verboseString: String =
    this.simpleString + "\n=== TRACES ===\n" + mergedPath.mkString("\n")

  lazy val paths: Seq[Seq[String]] = {
    val rootPath = Seq(view.format.shortNameOf(view.element))
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

  override lazy val children: Seq[ElementTreeNode[I]] = {

    _children.toList
      .sortBy(v => v.format.shortNameOf(v.element))
      .map {
        copyCutOffCyclic
      }
  }
}

object ElementTreeNode {

  case class Cyclic[I <: Impl](delegate: ElementTreeNode[I]) extends ElementTreeNode[I] {

    override def view: ElementView[I] = delegate.view

    override def visited = delegate.visited
    override lazy val prefix: String = delegate.prefix + "(cyclic)"
    override val _children: Seq[ElementView[I]] = Nil

    override implicit def copyImplicitly(v: ElementView[I]): ElementTreeNode[I] = delegate.copyImplicitly(v)
  }
}
