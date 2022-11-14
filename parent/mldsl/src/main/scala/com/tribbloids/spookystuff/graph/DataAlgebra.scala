package com.tribbloids.spookystuff.graph

trait DataAlgebra[T] {

  def eye: T = throw new UnsupportedOperationException(s"${this.getClass.getName} has no identity defined")

  def add(v1: T, v2: T): T

  object ForSome extends DataAlgebra[Option[T]] {

    override val eye = None

    override def add(v1: Option[T], v2: Option[T]): Option[T] = {
      (v1, v2) match {
        case (Some(x), Some(y)) => Some(DataAlgebra.this.add(x, y))
        case (Some(x), None)    => Some(x)
        case (None, Some(y))    => Some(y)
        case _                  => None
      }
    }
  }
}

object DataAlgebra {

  case class NoAmbiguity[T](eyeOpt: Option[T] = None) extends DataAlgebra[T] {

    override def eye: T = eyeOpt.getOrElse(super.eye)

    override def add(v1: T, v2: T): T = {
      if (v1 == v2) v1
      else throw new UnsupportedOperationException(s"conflict between data '$v1' and '$v2' which has identical IDs")
    }
  }

  case class MaxBy[T](fn: T => Long, eyeOpt: Option[T] = None) extends DataAlgebra[T] {

    override def eye: T = eyeOpt.getOrElse(super.eye)

    override def add(v1: T, v2: T): T = {

      Seq(v1, v2).maxBy(fn)
    }
  }

  case class Mutator[T <: Domain](
      nodeFn: T#NodeData => T#NodeData,
      edgeFn: T#EdgeData => T#EdgeData
  ) {}

  object Mutator {

    def identity[T <: Domain] = Mutator[T](Predef.identity, Predef.identity)
  }
}
