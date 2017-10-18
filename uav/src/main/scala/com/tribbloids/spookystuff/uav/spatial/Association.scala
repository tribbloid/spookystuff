package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.uav.spatial.point.{Coordinate, Location}
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.language.implicitConversions

object Association {

  implicit def fromTuple[T <: Spatial](tuple: (T, Anchor)) = Association[T](tuple._1, tuple._2)
}


case class Association[T <: Spatial](
                                      datum: T,
                                      anchor: Anchor
                                    ) extends TreeNode[Association[_]] {

  override def simpleString: String = {
    anchor match {
      case Location(_) =>
        datum.toString
      case _ =>
        s"${datum.toString} -+ $anchor"
    }
  }

  override def children: Seq[Association[Coordinate]] = {
    anchor match {
      case Location(seq) =>
        seq
      case _ =>
        Nil
    }
  }
}
