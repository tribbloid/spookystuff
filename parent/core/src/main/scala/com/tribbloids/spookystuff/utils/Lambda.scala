package com.tribbloids.spookystuff.utils

import scala.language.implicitConversions
import scala.runtime.ScalaRunTime

/**
  * Create to override the lame scala <functionN> toString format
  */
trait Lambda[-T, +R] extends Function1[T, R] with Product with Serializable {
  override def toString: String = ScalaRunTime._toString(this)
}

object Lambda {

  case class FromFunction[-T, +R](f: T => R) extends Lambda[T, R] {
    override def apply(v1: T): R = f(v1)
    override def toString: String = f.getClass.getSimpleName
  }

  implicit def fromFunction1[T, R](f: T => R): Lambda[T, R] =
    FromFunction(f)

  implicit def fromFunction2[T1, T2, R](f: (T1, T2) => R): Lambda[(T1, T2), R] =
    FromFunction((t: (T1, T2)) => f(t._1, t._2))
}
