package com.tribbloids.spookystuff

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

}
