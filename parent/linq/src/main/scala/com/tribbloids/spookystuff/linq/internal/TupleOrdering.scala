package com.tribbloids.spookystuff.linq.internal

import ai.acyclic.prover.commons.compat.TupleX
import ai.acyclic.prover.commons.compat.TupleX.*:

trait TupleOrdering {

  trait Impl[T <: TupleX] extends Ordering[T]
}

object TupleOrdering {

  // from shapeless.ops.hlists
  object Native extends TupleOrdering {

    implicit object empty extends Impl[TupleX.T0] {
      def compare(x: TupleX.T0, y: TupleX.T0): Int = 0
    }

    implicit def others[H, T <: TupleX](
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
