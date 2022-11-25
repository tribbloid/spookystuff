package com.tribbloids.spookystuff.tree

import org.apache.spark.sql.catalyst.trees.TreeNode

abstract class TreeView[BaseType <: TreeView[BaseType]] extends TreeNode[BaseType] {
  self: BaseType =>

  // due to the limitation of Spark TreeNode impl
  // every element must have only 1 row
  // this may change in the future once this class switch to an implementation with better support to multiple rows
  override protected def stringArgs: Iterator[Any] = Iterator.empty

  final override def verboseString(maxFields: Int): String = {
//    val argBlock =
//      if (stringArgs.isEmpty) ""
//      else stringArgs.mkString("[", ", ", "]")
    simpleString(maxFields)
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
