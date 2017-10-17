package com.tribbloids.spookystuff.uav.spatial

import org.apache.spark.sql.catalyst.trees.TreeNode

trait Association extends TreeNode[Association] {

  def data: SpatialData
  def anchor: Anchor

  override def simpleString: String = {
    anchor match {
      case Location(_) =>
        data.toString
      case _ =>
        s"${data.toString} -+ $anchor"
    }
  }

  override def children: Seq[CoordinateAssociation] = {
    anchor match {
      case Location(seq) =>
        seq
      case _ =>
        Nil
    }
  }
}
