package com.tribbloids.spookystuff

import ai.acyclic.prover.commons.graph.Arrow
import ai.acyclic.prover.commons.graph.local.Local
import ai.acyclic.prover.commons.graph.viz.Hierarchy

package object commons {

  type TreeView = TreeView.node

  object TreeView extends Local.Diverging.Tree.Codomain {

    abstract class node extends TreeView.Node_ {

      def children: Seq[TreeView]

      def nodeText: String

      final override def inductions: Seq[(Arrow.`~>`, TreeView)] = {

        children.map { v =>
          Arrow.`~>` -> v
        }
      }

      def hierarchyFormat: Hierarchy.Indent2.type = Hierarchy.Default

      final def treeString(): String = {
        hierarchyFormat.showNode(this).toString
      }
    }

//    object CanUnapply {
//
//      //    object Primary extends CanUnapply[TreeView] {
//      //      override def unapply(v: TreeView): Option[UnappliedForm] = ???
//      //    }
//      //
//      //    object Auxiliary extends CanUnapply[TreeView] {
//      //
//      //      override def unapply(v: TreeView): Option[UnappliedForm] = ???
//      //    }
//    }
  }

}
