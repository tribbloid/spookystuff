package com.tribbloids.spookystuff.tree

import org.apache.spark.sql.catalyst.trees.TreeNode

abstract class TreeView[BaseType <: TreeView[BaseType]] extends TreeNode[BaseType] {
  self: BaseType =>

  final override def verboseString(maxFields: Int): String = {
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
