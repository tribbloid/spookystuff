package com.tribbloids.spookystuff.linq.internal

import com.tribbloids.spookystuff.linq.{*:, Tuple}

trait TupleOrdering {

  trait Impl[T <: Tuple] extends Ordering[T]
}

object TupleOrdering {

  // from shapeless.ops.hlists
  object Native extends TupleOrdering {

    implicit object empty extends Impl[Tuple.Empty] {
      def compare(x: Tuple.Empty, y: Tuple.Empty): Int = 0
    }

    implicit def others[H, T <: Tuple](
        implicit
        hOrdering: Ordering[H],
        tOrdering: Impl[T]
    ): Impl[H *: T] =
      new Impl[H *: T] {
        def compare(x: H *: T, y: H *: T): Int = {
          val compareH = hOrdering.compare(x.head, y.head)

          if (compareH != 0) compareH else tOrdering.compare(x.tail, y.tail)
        }
      }

  }
}
