package com.tribbloids.spookystuff

import shapeless.{labelled, HNil}
import shapeless.labelled.{field, FieldType}
import shapeless.tag.@@

package object frameless {

  type XInt = Int with Singleton
  type XStr = String with Singleton

  type ->>[K, V] = FieldType[K, V]

  def ->>[K]: labelled.FieldBuilder[K] = field[K]

  // TODO: the following definition for Col will be obsolete in shapeless 2.4
  //  upgrade blocked by frameless

  type Col[T <: XStr] = Symbol @@ T

  def Col[T <: XStr](v: T): Col[T] = {

    Symbol(v).asInstanceOf[Col[T]]
  }

  type Col_->>[K <: XStr, V] = Col[K] ->> V

  class _ColValueConstructor[K <: XStr]() {

    def ->>[V](v: V): K Col_->> v.type = {
      v.asInstanceOf[K Col_->> v.type]
    }
  }
  def col[K <: XStr] = new _ColValueConstructor[K]()

  type Tuple = shapeless.HList
  object Tuple {

    type Empty = shapeless.HNil
    val empty: Empty = shapeless.HNil
  }
  type *:[+X, +Y <: Tuple] = shapeless.::[X, Y]

  implicit class TupleOps[T <: Tuple](self: T) {

    def *:[H](h: H): H *: T = h :: self
  }

}
