package org.apache.spark.ml.dsl

import org.apache.spark.ml.dsl.utils.StepTreeNodeRelay
import org.apache.spark.sql.catalyst.trees.TreeNode

trait StepTreeNode[BaseType <: StepTreeNode[BaseType]] extends TreeNode[StepTreeNode[BaseType]] with StepTreeNodeRelay.ToReprMixin {

  val self: StepLike

  lazy val paths: Seq[Seq[String]] = {
    val rootPath = Seq(self.name)
    if (children.nonEmpty) {
      children.flatMap{
        child =>
          child.paths.map(_ ++ rootPath)
      }
    }
    else Seq(rootPath)
  }

  lazy val mergedPath: Seq[String] = {

    val numPaths = paths.map(_.size)
    assert(numPaths.nonEmpty, "impossible")
    val result = {
      val maxBranchLength = numPaths.max
      val commonAncestorLength = maxBranchLength.to(0, -1).find{
        v =>
          paths.map(_.slice(0, v)).distinct.size == 1
      }.getOrElse(0)
      val commonAncestor = paths.head.slice(0, commonAncestorLength)

      val commonParentLength = maxBranchLength.to(0, -1).find{
        v =>
          paths.map(_.reverse.slice(0, v)).distinct.size == 1
      }.getOrElse(0)
      val commonParent = paths.head.reverse.slice(0, commonParentLength).reverse

      if (commonAncestor.size + commonParent.size > maxBranchLength) commonParent
      else commonAncestor ++ commonParent
    }
    result
  }
}
