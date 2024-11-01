package com.tribbloids.spookystuff

import shapeless.labelled
import shapeless.labelled.{field, FieldType}
import shapeless.tag.@@

package object linq {

  type XInt = Int with Singleton
  type XStr = String with Singleton

  type Tuple = shapeless.HList
  object Tuple {

    type Empty = shapeless.HNil
    val empty: Empty = shapeless.HNil

  }

  type *:[+X, +Y <: Tuple] = shapeless.::[X, Y]
  implicit class TupleOps[T <: Tuple](self: T) {

    def *:[H](h: H): H *: T = h :: self
  }

  type T1[T] = T *: Tuple.Empty

  type ->>[K, V] = FieldType[K, V]

  def ->>[K]: labelled.FieldBuilder[K] = field[K]

  // TODO: the following definition for Col will be obsolete in shapeless 2.4
  //  upgrade blocked by frameless

  type ColumnTag[T <: XStr] = Symbol @@ T

  def ColumnTag[T <: XStr](v: T): ColumnTag[T] = {

    Symbol(v).asInstanceOf[ColumnTag[T]]
  }

  type :=[K <: XStr, V] = ColumnTag[K] ->> V
}
