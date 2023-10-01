package com.tribbloids.spookystuff.extractors

// only a placeholder, function serialization problem is long gone TODO: remove
object Unlift {

  @inline def apply[T, R](fn: T => Option[R]): PartialFunction[T, R] = Function.unlift(fn)
}
