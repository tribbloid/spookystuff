package com.tribbloids.spookystuff.graph

trait DataAlgebra[T] {

  val eye: T

  def combine(v1: T, v2: T): T

  def combineMonads(v1: Option[T], v2: Option[T]): Option[T] = {
    (v1, v2) match {
      case (Some(x), Some(y)) => Some(combine(x, y))
      case (Some(x), None)    => Some(x)
      case (None, Some(y))    => Some(y)
      case _                  => None
    }
  }
}
