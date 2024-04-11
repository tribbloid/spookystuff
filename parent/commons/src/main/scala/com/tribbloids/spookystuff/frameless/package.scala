package com.tribbloids.spookystuff

import shapeless.labelled.{field, FieldType}
import shapeless.tag.@@

package object frameless {

  // TODO: definition here will be obsolete in shapeless 2.4
  //  upgrade blocked by frameless

  type XInt = Int with Singleton
  type XStr = String with Singleton

  type Col[T <: XStr] = Symbol @@ T

  def Col[T <: XStr](v: T): Col[T] = {

    Symbol(v).asInstanceOf[Col[T]]
  }

  type ->>[K, V] = FieldType[K, V]

  def ->>[K] = field[K]

  type Col_->>[T <: XStr, V] = Col[T] ->> V

  def Col_->>[T <: XStr](v: Any): T Col_->> v.type = {
    v.asInstanceOf[T Col_->> v.type]
  }

  type Tuple = shapeless.HList
  object Tuple {

    type Empty = shapeless.HNil
    val Empty = shapeless.HNil
  }
  type *:[+X, +Y <: Tuple] = shapeless.::[X, Y]

  implicit class TupleOps[T <: Tuple](self: T) {

    def *:[H](h: H): H *: T = h :: self
  }

}
