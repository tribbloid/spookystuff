package org.apache.spark.ml.dsl.utils

import scala.language.implicitConversions

/**
  * a magnet wrapper for Option
  */
case class Nullable[+T](asOption: Option[T]) extends Nullable.Magnet[T] {}

object Nullable {

  trait Magnet[+T] extends Product with Serializable {

    def asOption: Option[T]
  }

  implicit def fromV[T](v: T): Nullable[T] = Nullable(Option(v))
  implicit def fromOpt[T](v: Option[T]): Nullable[T] = Nullable(v)

  implicit def toOpt[T](magnet: Nullable[T]): Option[T] = magnet.asOption

  case class NOT[+T](private val v: T) extends Magnet[T] {
    {
      require(v != null, "value cannot be null")
    }

    override def asOption: Option[T] = Some(v)
  }

  object NOT {

    implicit def fromV[T](v: T): NOT[T] = NOT(v)

    implicit def toV[T](magnet: NOT[T]): T = magnet.v
  }
}
