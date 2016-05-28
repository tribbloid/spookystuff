package com.tribbloids.spookystuff.extractors

/**
  * Created by peng on 13/06/16.
  */
//Equivalent to Function.unlift, except being Serializable
case class Unlift[-T, +R](
                           liftFn: T => Option[R]
                         ) extends AbstractPartialFunction[T, R]{

  override final def isDefinedAt(x: T): Boolean = liftFn(x).isDefined

  override final def applyOrElse[A1 <: T, B1 >: R](x: A1, default: A1 => B1): B1 = {
    val z = liftFn(x)
    z.getOrElse(default(x))
  }

  override final def lift: Function1[T, Option[R]] = liftFn
}

case class Raw[-T, +R](
                        gen: T => R
                      ) extends PartialFunctionWrapper[T, R] {

  val self: scala.PartialFunction[T, R] = gen match {
    case pf: scala.PartialFunction[T, R] =>
      pf
    case _ =>
      PartialFunction(gen)
  }
}