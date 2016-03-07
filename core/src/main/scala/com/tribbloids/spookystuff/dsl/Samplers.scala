package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.row.Sampler

import scala.util.Random

/**
  * Created by peng on 06/04/16.
  */
object Samplers {

  case class FirstN(n: Int) extends Sampler[Any] {

    def apply(v: Iterable[(Any, Int)]) =
      v.slice(0, n)
  }

  case class DownsamplingByRatio(ratio: Double) extends Sampler[Any] {
    def apply(v: Iterable[(Any, Int)]) =
      Random
        .shuffle(v)
        .slice(0, (ratio * v.size).toInt)
  }

  case class DownsamplingToN(n: Int) extends Sampler[Any] {
    def apply(v: Iterable[(Any, Int)]) =
      Random
        .shuffle(v)
        .slice(0, n)
  }
}