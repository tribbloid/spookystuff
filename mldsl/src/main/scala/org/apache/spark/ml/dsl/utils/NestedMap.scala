package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.utils.CommonUtils

import scala.collection.mutable

//Strongly typed! Should totally replace Nested
//TreeNode with names!
trait NestedMapLike[V, Self <: NestedMapLike[V, Self]] extends mutable.ListMap[String, Either[V, Self]] {

//  def self: mutable.ListMap[String, Either[T, Self]]
}

case class NestedMap[V](
    )
    extends NestedMapLike[V, NestedMap[V]] {

  def leafMap: Map[String, V] = {
    val current: Map[String, V] = this.flatMap {
      case (k, Left(v)) =>
        Some(k -> v)
      case _ =>
        None
    }.toMap

    val nested: Map[String, V] = this.flatMap {
      case (k, Right(nn)) =>
        nn.leafMap.map {
          case (kk, v) =>
            CommonUtils./:/(k, kk) -> v
        }
      case _ =>
        Nil
    }.toMap

    current ++ nested
  }

//  override def size = self.size
}
