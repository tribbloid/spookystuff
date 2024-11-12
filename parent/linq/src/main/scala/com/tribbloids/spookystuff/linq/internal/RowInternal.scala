package com.tribbloids.spookystuff.linq.internal

import ai.acyclic.prover.commons.function.hom.Hom
import com.tribbloids.spookystuff.linq.*
import com.tribbloids.spookystuff.linq.Linq.{Row, named}
import com.tribbloids.spookystuff.linq.Tuple.Empty
import shapeless.ops.record.Keys

case class RowInternal[L <: Tuple](
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
      .foldRight[Tuple](Tuple.empty) { (s, x) =>
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
}

object RowInternal {

  // TODO: remove, nameless columns is not supported in RecordEncoderField
  //  object ofArgs extends ProductArgs {
  //    def applyProduct[L <: Tuple](list: L): TypedRow[L] = fromTuple(list)
  //  }

  def ofTuple[L <: Tuple](
      data: L
  ): Row[L] = {

    val cells = data.runtimeList

    new Row[L](cells.to(Vector))
  }

  def ofShapelessTagged[K <: XStr, V](
      v: K := V
  ): Row[(K := V) *: Empty] = ofTuple(v *: Tuple.empty)

  sealed protected trait ofData_Imp0 extends Hom.Poly {

    implicit def fromValue[V]: V Target Row[("value" := V) *: Tuple.Empty] = at[V] { v =>
      ofTuple((named["value"] := v) *: Tuple.empty)
    }
  }

  object ofData extends ofData_Imp0 {

    implicit def id[L <: Tuple]: Row[L] Target Row[L] = at[Row[L]] {
      identity[Row[L]]
    }
  }

}
