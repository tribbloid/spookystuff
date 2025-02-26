package com.tribbloids.spookystuff.linq.internal

import ai.acyclic.prover.commons.compat.NamedTupleX.:=
import ai.acyclic.prover.commons.compat.TupleX.{*:, T0}
import ai.acyclic.prover.commons.compat.{Key, TupleX, XStr}
import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.linq.Linq.Row
import shapeless.ops.record.Keys

import scala.collection.immutable.ListMap

case class RowInternal[L <: TupleX](
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
      keys.runtimeList.zip(runtimeVector): _*
    )

  }
}

object RowInternal {

  def ofTuple[L <: TupleX](
      data: L
  ): Row[L] = {

    val cells = data.runtimeList

    new Row[L](cells.to(Vector))
  }

  def ofTagged[K <: XStr, V](
      v: K := V
  ): Row[(K := V) *: T0] = ofTuple(v *: TupleX.T0)

  sealed protected trait ofData_Imp0 extends Hom.Poly {

    implicit def fromValue[V]: V |- Row[("value" := V) *: TupleX.T0] = at[V] { v =>
      ofTuple((Key["value"] := v) *: TupleX.T0)
    }
  }

  object ofData extends ofData_Imp0 {

    implicit def id[L <: TupleX]: Row[L] |- Row[L] = at[Row[L]] {
      identity[Row[L]]
    }
  }

}
