package com.tribbloids.spookystuff.graph

trait Impl {

  type T <: Domain
  type G <: StaticGraph[T]
}

object Impl {

  trait Sugars[I <: Impl] extends Algebra.Sugars[I#T] {
    type T = I#T
    type G = I#G

    type _DSL = DSL[I]
    type _ElementTreeNode = ElementTreeNode[I]
    type _ElementView = ElementView[I]
  }
}
