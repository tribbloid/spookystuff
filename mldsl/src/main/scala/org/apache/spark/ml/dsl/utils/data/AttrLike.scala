package org.apache.spark.ml.dsl.utils.data

import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.ml.dsl.utils.?

import scala.util.Try

trait AttrLike[T] extends Serializable with IDMixin {

  def primaryName: String
  def aliases: List[String]

  final lazy val allNames: Seq[String] = (Seq(primaryName) ++ aliases).distinct

  def ->(v: T): Magnets.KV[T] = {

    Magnets.KV[T](this.primaryName, Some(v))
  }

  def -?>(vOpt: T ? _): Magnets.KV[T] = {

    Magnets.KV[T](this.primaryName, vOpt.asOption)
  }

  def tryGet: Try[T]
  def get: Option[T] = tryGet.toOption
  def value: T = tryGet.get

  override def _id: Any = this.allNames -> get
}

object AttrLike {

//  implicit def toV[T](attr: AttrLike[T]): T = attr.value
}
