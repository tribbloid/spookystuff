package com.tribbloids.spookystuff.commons.data

import ai.acyclic.prover.commons.multiverse.{CanEqual, Projection}
import ai.acyclic.prover.commons.util.Magnet.OptionMagnet

import scala.util.Try

trait AttrLike[T] extends Serializable with Projection.Equals {

  {
    canEqualProjections += CanEqual.Native.on(this.allNames -> get)
  }

  def name: String
  def aliases: List[String]

  final lazy val allNames: Seq[String] = (Seq(name) ++ aliases).distinct

  def ->(v: T): Magnets.AttrValueMag[T] = {

    Magnets.AttrValueMag[T](this.name, Some(v))
  }

  def -?>(vOpt: OptionMagnet[T]): Magnets.AttrValueMag[T] = {

    Magnets.AttrValueMag[T](this.name, vOpt)
  }

  def tryGet: Try[T]

  final def get: Option[T] = tryGet.toOption
  final def value: T = tryGet.get
}

object AttrLike {

//  implicit def toV[T](attr: AttrLike[T]): T = attr.value
}
