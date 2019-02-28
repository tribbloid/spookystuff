package com.tribbloids.spookystuff.graph

trait DataAlgebra[T] {

  val eye: T

  def combine(v1: T, v2: T): T
}
