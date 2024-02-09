package com.tribbloids.spookystuff.utils.data

import ai.acyclic.prover.commons.same.EqualBy
import org.apache.spark.ml.dsl.utils.?

import scala.util.Try

trait AttrLike[T] extends Serializable with EqualBy {

  def name: String
  def aliases: List[String]

  final lazy val allNames: Seq[String] = (Seq(name) ++ aliases).distinct

  def ->(v: T): Magnets.AttrValueMag[T] = {

    Magnets.AttrValueMag[T](this.name, Some(v))
  }

  def -?>(vOpt: T `?` _): Magnets.AttrValueMag[T] = {

    Magnets.AttrValueMag[T](this.name, vOpt.asOption)
  }

  def tryGet: Try[T]

  final def get: Option[T] = tryGet.toOption
  final def value: T = tryGet.get

  override def samenessDelegatedTo: Any = this.allNames -> get
}

object AttrLike {

//  implicit def toV[T](attr: AttrLike[T]): T = attr.value
}
