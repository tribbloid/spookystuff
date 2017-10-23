package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.uav.spatial.point.{Coordinate, Location}
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.language.implicitConversions

object Association {

  implicit def fromTuple[T <: Spatial](tuple: (T, Anchor)) = Association[T](tuple._1, tuple._2)
}


case class Association[+T <: Spatial](
                                      datum: T,
                                      anchor: Anchor
                                    ) extends TreeNode[Association[Spatial]] {

  override def simpleString: String = {
    datum.toString + " -+ " + anchor.name
  }

  override def children: Seq[Association[Spatial]] = {
    anchor match {
      case Location(seq, _) =>
        seq
      case _ =>
        Nil
    }
  }

//  def simplifyOnce = {
//    children.map {
//      child =>
//
//    }
//  }
//
//  def simplify: Association[T] = {
//    this.transformUp {
//      case Association(coord: Coordinate, Location(definedBy, tag)) =>
//        val projected = definedBy.map {
//          chil
//        }
//    }
//    ???
//  }
}
