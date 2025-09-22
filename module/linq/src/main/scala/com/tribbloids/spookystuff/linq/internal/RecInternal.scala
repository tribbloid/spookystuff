package com.tribbloids.spookystuff.linq.internal

import ai.acyclic.prover.commons.compat.TupleX.*:
import ai.acyclic.prover.commons.compat.TupleX
import shapeless.ops.record.Keys

import scala.collection.immutable.ListMap

case class RecInternal[L <: TupleX](
    runtimeVector: Vector[Any]
) {

  @transient def head[H](
      implicit
      ev: L <:< (H *: ?)
  ): H = {

    runtimeVector.head.asInstanceOf[H]
  }

  @transient lazy val repr: L = {
    runtimeVector
      .foldRight[TupleX](TupleX.T0) { (s, x) =>
        import TupleX._ops

        s *: x
      }
      .asInstanceOf[L]
  }

  import shapeless.record.*

  type Repr = L

  def keys(
      implicit
      ev: Keys[L]
  ): Keys[L]#Out = repr.keys

  def runtimeMap(
      implicit
      ev: Keys[L]
  ) = {

    ListMap(
      keys.runtimeList.zip(runtimeVector)*
    )
  }
}

object RecInternal {}
