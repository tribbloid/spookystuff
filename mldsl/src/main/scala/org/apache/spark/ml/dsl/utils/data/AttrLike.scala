package org.apache.spark.ml.dsl.utils.data

import org.apache.spark.ml.dsl.utils.{?, Nullable}

import scala.language.implicitConversions
import scala.util.Try

trait AttrLike[T] extends Serializable {

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
}

object AttrLike {

//  implicit def toV[T](attr: AttrLike[T]): T = attr.value
}
