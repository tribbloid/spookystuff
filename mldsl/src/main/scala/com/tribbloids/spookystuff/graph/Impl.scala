package com.tribbloids.spookystuff.graph

import scala.language.{higherKinds, implicitConversions}

trait Impl {

  type DD <: Domain
  type GProto[T <: Domain] <: StaticGraph[T]

  type GG = GProto[DD]
}

object Impl {

  trait Sugars[I <: Impl] extends Algebra.Sugars[I#DD] {
    final type DD = I#DD
    final type GG = I#GG

    final type _DSL = DSL[I]
    final type _ElementTreeNode = ElementTreeNode[I]
    final type _ElementView = ElementView[I]

    //TODO: this abomination is to mitigate the weak type inference system of scala, should be removed in later version
    implicit def upcast(v: I#GG): StaticGraph[DD] = v
  }
}
