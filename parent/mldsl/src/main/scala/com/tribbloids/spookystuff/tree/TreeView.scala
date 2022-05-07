package com.tribbloids.spookystuff.tree

import org.apache.spark.sql.catalyst.trees.TreeNode

abstract class TreeView[BaseType <: TreeView[BaseType]] extends TreeNode[BaseType] {
  self: BaseType =>

  // due to the limitation of Spark TreeNode impl
  // every element must have only 1 row
  // this may change in the future once this class switch to an implementation with better support to multiple rows
  protected def argStrings: Seq[String] = Nil

  final override def verboseString(maxFields: Int): String = {
    val argBlock =
      if (argStrings.isEmpty) ""
      else argStrings.mkString("[", ", ", "]")
    simpleString(maxFields) + argBlock
  }

  final override def simpleStringWithNodeId(): String = simpleString(0)

}

object TreeView {

  trait Immutable[BaseType <: Immutable[BaseType]] extends TreeView[BaseType] {
    self: BaseType =>

    final override def withNewChildrenInternal(newChildren: IndexedSeq[BaseType]): BaseType = {
      ???
      // unsupported,
    }
  }
}
