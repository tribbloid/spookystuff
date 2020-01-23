package com.tribbloids.spookystuff.extractors

import scala.runtime.AbstractPartialFunction

//this entire file is created because default result of .lift & .unlift are not serializable
trait PartialFunctionWrapper[-T, +R] extends PartialFunction[T, R] {
  def partialFunction: scala.PartialFunction[T, R]

  override final def isDefinedAt(x: T): Boolean = partialFunction.isDefinedAt(x)
  override def apply(v1: T) = partialFunction.apply(v1)
  override final def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 =
    partialFunction.applyOrElse(x, default)

  override final def lift: Function1[T, Option[R]] = {

    this.Lift
//    partialFunction match {
//      case ul: Unlift[T, R] => ul.lift
//      case _                => this.Lift
//    }
  }

  case object Lift extends Function1[T, Option[R]] {

    def apply(v1: T): Option[R] = {
      val fO: scala.PartialFunction[T, Option[R]] = PartialFunctionWrapper.this.andThen[Option[R]](v => Some(v))
      fO.applyOrElse(v1, (_: T) => None)
    }
  }
}

// WARNING: DO NOT CHANGE! result of PartialFunctions.unlift in scala 2.11 is NOT serializable!
case class Unlift[-T, +R](
    liftFn: T => Option[R]
) extends AbstractPartialFunction[T, R] {

  override final def isDefinedAt(x: T): Boolean = liftFn(x).isDefined

  override final def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = {
    val z = liftFn(x)
    z.getOrElse(default(x))
  }

  override final def lift: Function1[T, Option[R]] = liftFn
}

case class Partial[-T, +R](
    fn: T => R
) extends PartialFunctionWrapper[T, R] {

  val partialFunction: scala.PartialFunction[T, R] = fn match {
    case pf: scala.PartialFunction[T, R] => pf
    case _                               => { case v => fn(v) }
  }
}
