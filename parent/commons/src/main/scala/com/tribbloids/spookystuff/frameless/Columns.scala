package com.tribbloids.spookystuff.frameless

import ai.acyclic.prover.commons.function.hom.Hom
import shapeless.SingletonProductArgs
import shapeless.ops.hlist.Mapper

@Deprecated // not working TODO: remove
class Columns[T <: Tuple](
    repr: T
) {}

@Deprecated // not working
object Columns extends SingletonProductArgs {

  object Singleton2Col extends Hom.Poly {

    implicit def caseSymbol[T <: XStr]: T Target Col[T] =
      at[T] { s =>
        Col(s)
      }
  }

  def applyProduct[L <: Tuple](list: L)(
      implicit
      ev: Mapper[
        Singleton2Col.asShapelessPoly1.type,
        L
      ]
  ): Columns[ev.Out] = {

    val cols = list.map(Singleton2Col.asShapelessPoly1)
    new Columns(cols)
  }
}
