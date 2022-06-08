package com.tribbloids.spookystuff.uav.spatial

import com.tribbloids.spookystuff.uav.spatial.point.Location
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.language.implicitConversions

object GeoRef {

  implicit def fromTuple[T <: Spatial](tuple: (T, Anchor)) = GeoRef[T](tuple._1, tuple._2)
}

case class GeoRef[+T <: Spatial](
    geom: T,
    anchor: Anchor
) extends TreeNode[GeoRef[Spatial]] {

  override def simpleString: String = {
    geom.toString + " -+ " + anchor.name
  }

  override def verboseString: String = simpleString

  override def children: Seq[GeoRef[Spatial]] = {
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
