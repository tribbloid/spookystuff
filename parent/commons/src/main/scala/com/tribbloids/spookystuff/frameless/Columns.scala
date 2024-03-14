package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.Hom
import shapeless.ops.hlist.Mapper
import shapeless.SingletonProductArgs

@Deprecated // not working TODO: remove
class Columns[T <: Tuple](
    repr: T
) {}

@Deprecated // not working
object Columns extends SingletonProductArgs {

  object Singleton2Col extends Hom.Poly {

    implicit def caseSymbol[T <: String with Singleton]: T =>> Col[T] =
      at[T] { s =>
        Col(s)
      }
  }

  def applyProduct[L <: Tuple](list: L)(
      implicit
      ev: Mapper[
        Singleton2Col.asShapeless.type,
        L
      ]
  ): Columns[ev.Out] = {

    val cols = list.map(Singleton2Col.asShapeless)
    new Columns(cols)
  }
}
