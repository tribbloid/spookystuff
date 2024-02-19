package org.apache.spark.ml.dsl.utils

import scala.language.implicitConversions

sealed trait NullSafeMagnet[+T] extends Product with Serializable {

  def asOption: Option[T]
}

/**
  * a magnet wrapper for Option
  */
object NullSafeMagnet {
  // TODO: should be a special case of
  //  the following design can reduce overhead and improve Scala 3 readiness:
  //  https://github.com/sjrd/scala-unboxed-option

  /**
    * capability mixin
    */
  sealed trait Cap extends Serializable

  case class CanBeNull[T, +M <: Cap](private var _self: Option[T]) extends NullSafeMagnet[T] {

    override def asOption: Option[T] = _self
  }

  trait CanBeNull_Imp0 {}

  object CanBeNull extends CanBeNull_Imp0 {

    implicit def fromV[T, M <: Cap](v: T): CanBeNull[T, M] = CanBeNull[T, M](Option(v))

    implicit def fromOpt[T, M <: Cap](v: Option[T]): CanBeNull[T, M] = CanBeNull[T, M](v)

    implicit def toOption[T](magnet: CanBeNull[T, _]): Option[T] = magnet.asOption
  }

  case class NotNull[T, +M <: Cap](var value: T) extends NullSafeMagnet[T] {

    {
      validate(value)
    }

    def validate(value: T): Unit = {
      require(value != null, "value cannot be null")
    }

    override def asOption: Some[T] = Some(value)
  }

  object NotNull extends CanBeNull_Imp0 {

    implicit def fromV[T, M <: Cap](v: T): NotNull[T, M] = NotNull[T, M](v)

    implicit def fromSome[T, M <: Cap](v: Some[T]): NotNull[T, M] = NotNull[T, M](v.get)

    implicit def toOption[T](magnet: NotNull[T, _]): Some[T] = Some(magnet.value)

    implicit def toV[T](magnet: NotNull[T, _]): T = magnet.value
  }
}
