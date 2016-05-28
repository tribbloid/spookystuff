package com.tribbloids.spookystuff.extractors

trait PartialFunctionWrapper[-T, +R] extends PartialFunction[T, R] {
  def self: scala.PartialFunction[T, R]

  override final def isDefinedAt(x: T): Boolean = self.isDefinedAt(x)
  override def apply(v1: T) = self.apply(v1)
  override final def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = self.applyOrElse(x, default)
  override final def lift: Function1[T, Option[R]] = self match {
    case ul: Unlift[T, R] => ul.lift
    case _ => this.Lift
  }

  case object Lift extends Function1[T, Option[R]]{

    def apply(v1: T): Option[R] = {
      val fO: scala.PartialFunction[T, Option[R]] = PartialFunctionWrapper.this.andThen[Option[R]](v => Some(v))
      fO.applyOrElse(v1, (_: T) => None)
    }
  }
}