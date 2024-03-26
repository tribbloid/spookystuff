package com.tribbloids.spookystuff.frameless

import shapeless.ops.record.Keys

case class TypedRowInternal[L <: Tuple](repr: L) {

  import shapeless.record._

  type Repr = L

  def keys(
      implicit
      ev: Keys[L]
  ): Keys[L]#Out = repr.keys
}

object TypedRowInternal {}
