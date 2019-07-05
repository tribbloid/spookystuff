package org.apache.spark.ml.dsl.utils

import scala.language.implicitConversions

/**
  * a magnet wrapper for Option
  */
object NullSafe {

  def apply[T](asOption: Option[T]): Immutable[T] = Immutable(asOption)

  trait Magnetic[+T] extends Product with Serializable {

    def asOption: Option[T]

    def value: T = asOption.get
  }

  case class Immutable[+T](asOption: Option[T]) extends Magnetic[T] {}

  object Immutable {

    implicit def fromV[T](v: T): Immutable[T] = Immutable(Option(v))
    implicit def fromOpt[T](v: Option[T]): Immutable[T] = Immutable(v)

    implicit def toOpt[T](magnet: Immutable[T]): Option[T] = magnet.asOption
  }

  case class Mutable[T](var asOption: Option[T]) extends Magnetic[T] {

    def value_=(v: T): Unit = {
      asOption = Option(v)
    }
  }

  object Mutable {

    implicit def fromV[T](v: T): Mutable[T] = Mutable(Option(v))
    implicit def fromOpt[T](v: Option[T]): Mutable[T] = Mutable(v)

    implicit def toOpt[T](magnet: Mutable[T]): Option[T] = magnet.asOption
  }

  case class NOT[+T](private val v: T) extends Magnetic[T] {
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
