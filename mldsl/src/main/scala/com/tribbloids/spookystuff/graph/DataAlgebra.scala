package com.tribbloids.spookystuff.graph

trait DataAlgebra[T] {

  def eye: T = throw new UnsupportedOperationException(s"${this.getClass.getName} has no identity")

  def plus(v1: T, v2: T): T

  object Monadic extends MonadicAlgebra[T] {

    override def plus(v1: Option[T], v2: Option[T]): Option[T] = {
      (v1, v2) match {
        case (Some(x), Some(y)) => Some(DataAlgebra.this.plus(x, y))
        case (Some(x), None)    => Some(x)
        case (None, Some(y))    => Some(y)
        case _                  => None
      }
    }
  }
}

trait MonadicAlgebra[T] extends DataAlgebra[Option[T]] {

  override val eye = None
}
