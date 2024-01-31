package org.apache.spark.ml.dsl.utils

import scala.language.implicitConversions

/**
  * a magnet wrapper for Option
  */
object NullSafety {

//  object Val extends IsMutable
  final class Var extends Serializable

//  def apply[T](asOption: Option[T]): Immutable[T] = Immutable(asOption)

  trait Magnet[+T] extends Product with Serializable {

    def asOption: Option[T]
  }

  case class CanBeNull[T, +M](private var _self: Option[T]) extends Magnet[T] {

    override def asOption: Option[T] = _self

    def :=(v: T)(
        implicit
        ev: M <:< Var
    ): Unit = {
      _self = Option(v)
    }
  }

  trait LowLevelImplicits {}

  object CanBeNull extends LowLevelImplicits {

    implicit def fromV[T, M](v: T): CanBeNull[T, M] = CanBeNull[T, M](Option(v))

    implicit def fromOpt[T, M](v: Option[T]): CanBeNull[T, M] = CanBeNull[T, M](v)

    implicit def toOption[T](magnet: CanBeNull[T, _]): Option[T] = magnet.asOption
  }

  case class CannotBeNull[T, +M](var value: T) extends Magnet[T] {

    {
      validate(value)
    }

    def validate(value: T): Unit = {
      require(value != null, "value cannot be null")
    }

    override def asOption: Some[T] = Some(value)

    def :=(value: T)(
        implicit
        ev: M <:< Var
    ): Unit = {
      validate(value)

      this.value = value
    }
  }

  object CannotBeNull extends LowLevelImplicits {

    implicit def fromV[T, M](v: T): CannotBeNull[T, M] = CannotBeNull[T, M](v)

    implicit def fromSome[T, M](v: Some[T]): CannotBeNull[T, M] = CannotBeNull[T, M](v.get)

    implicit def toOption[T](magnet: CannotBeNull[T, _]): Some[T] = Some(magnet.value)

    implicit def toV[T](magnet: CannotBeNull[T, _]): T = magnet.value
  }
}
